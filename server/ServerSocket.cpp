/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#include "ServerSocket.hpp"
#include "DirectoryEntry.hpp"

#include <commitmanager/ErrorCode.hpp>
#include <commitmanager/MessageTypes.hpp>
#include <commitmanager/HashRing.hpp>

#include <boost/algorithm/string/join.hpp>

#include <crossbow/enum_underlying.hpp>
#include <crossbow/logger.hpp>

#include <chrono>

namespace tell {
namespace commitmanager {

void ServerSocket::onRequest(crossbow::infinio::MessageId messageId, uint32_t messageType,
        crossbow::buffer_reader& message) {
    manager().onMessage(this, messageId, messageType, message);
}

ServerManager::ServerManager(crossbow::infinio::InfinibandService& service, const ServerConfig& config)
        : Base(service, config.port),
          mProcessor(service.createProcessor()),
          mMaxBatchSize(config.maxBatchSize),
          mDirectoryVersion(0),
          mNodeRing(1) {} // @TODO

ServerSocket* ServerManager::createConnection(crossbow::infinio::InfinibandSocket socket,
        const crossbow::string& data) {
    // The length of the data field may be larger than the actual data sent (behavior of librdma_cm) so check the
    // handshake against the substring
    auto& handshake = handshakeString();
    if (data.size() < handshake.size() || data.substr(0, handshake.size()) != handshake) {
        LOG_ERROR("Connection handshake failed");
        socket->reject(crossbow::string());
        return nullptr;
    }

    LOG_INFO("%1%] New client connection", socket->remoteAddress());
    return new ServerSocket(*this, *mProcessor, std::move(socket), mMaxBatchSize);
}

void ServerManager::onMessage(ServerSocket* con, crossbow::infinio::MessageId messageId, uint32_t messageType,
        crossbow::buffer_reader& message) {
#ifdef NDEBUG
#else
    LOG_TRACE("MID %1%] Handling request of type %2%", messageId.userId(), messageType);
    auto startTime = std::chrono::steady_clock::now();
#endif

    switch (messageType) {

    case crossbow::to_underlying(WrappedResponse::START): {
        handleStartTransaction(con, messageId, message);
    } break;

    case crossbow::to_underlying(WrappedResponse::COMMIT): {
        handleCommitTransaction(con, messageId, message);
    } break;

    case crossbow::to_underlying(WrappedResponse::REGISTER_NODE): {
        handleRegisterNode(con, messageId, message);
    } break;

    case crossbow::to_underlying(WrappedResponse::UNREGISTER_NODE): {
        handleUnregisterNode(con, messageId, message);
    } break;

    case crossbow::to_underlying(WrappedResponse::TRANSFER_OWNERSHIP): {
        handleTransferOwnership(con, messageId, message);
    } break;

    default: {
        con->writeErrorResponse(messageId, error::unkown_request);
    } break;
    }

#ifdef NDEBUG
#else
    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
    LOG_TRACE("MID %1%] Handling request took %2%ns", messageId.userId(), duration.count());
#endif
}

void ServerManager::handleStartTransaction(ServerSocket* con, 
                                           crossbow::infinio::MessageId messageId, 
                                           crossbow::buffer_reader& message) {
    auto readonly = (message.read<uint8_t>() != 0x0u);
    if (!mCommitManager.startTransaction(readonly)) {
        con->writeErrorResponse(messageId, error::transaction_limit_reached);
        return;
    }

    crossbow::string nodeInfo = boost::algorithm::join(getMatchingHosts("STORAGE"), ";");
    crossbow::string bootstrapInfo = boost::algorithm::join(getBootstrappingHosts("STORAGE"), ";");

    auto messageLength = mCommitManager.serializedLength() + sizeof(uint64_t) + 2*sizeof(uint32_t) + nodeInfo.size() + bootstrapInfo.size();
    
    con->writeResponse(messageId, ResponseType::START, messageLength, 
        [this, nodeInfo, bootstrapInfo] (crossbow::buffer_writer& message, std::error_code& /* ec */) {
            mCommitManager.serializeSnapshot(message);

            // Write most recent directory version
            message.write<uint64_t>(mDirectoryVersion);

            // Write peer addresses
            message.write<uint32_t>(nodeInfo.size());
            message.write(nodeInfo.data(), nodeInfo.size());

            // Write addresses of bootstrapping peers
            message.write<uint32_t>(bootstrapInfo.size());
            message.write(bootstrapInfo.data(), bootstrapInfo.size()); 
        }
    );
}

void ServerManager::handleCommitTransaction(ServerSocket* con, crossbow::infinio::MessageId messageId,
        crossbow::buffer_reader& message) {
    auto version = message.read<uint64_t>();

    auto succeeded = mCommitManager.commitTransaction(version);

    uint32_t messageLength = sizeof(uint8_t);
    con->writeResponse(messageId, ResponseType::COMMIT, messageLength, [succeeded]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint8_t>(succeeded ? 0x1u : 0x0u);
    });
}

/**
 * @brief Updates the node directory with new status information from a node.
 */
void ServerManager::handleRegisterNode(ServerSocket *con, 
                                       crossbow::infinio::MessageId messageId, 
                                       crossbow::buffer_reader &message) {
    uint64_t version = message.read<uint64_t>();

    uint32_t hostSize = message.read<uint32_t>();
    crossbow::string host(message.read(hostSize), hostSize);
    
    uint32_t tagSize = message.read<uint32_t>();
    crossbow::string tag(message.read(tagSize), tagSize);
    
    // Get directory info
    crossbow::string nodeInfo = boost::algorithm::join(getMatchingHosts(tag), ";");

    // Register the node
    LOG_INFO("Registering node @ %1% (last was %2%)", version, mDirectoryVersion);
    if (version > mDirectoryVersion) {
        mDirectoryVersion = version;
    }

    mDirectory[host] = std::move(std::unique_ptr<DirectoryEntry>(new DirectoryEntry(host, tag)));

    if (mNodeRing.isEmpty()) {
        // Special case for the very first node: It has to be inserted
        // before calculating ranges, so pointers don't wrap around in the ring.
        mNodeRing.insertNode(host, host);
        mDirectory[host]->isBootstrapping = false;      
    }
    
    std::vector<Partition> ranges = mNodeRing.getRanges(host);

    mNodeRing.insertNode(host, host);

    // Write response
    uint32_t messageLength = sizeof(uint32_t) + nodeInfo.size() + sizeof(uint32_t);
    for (auto const &range : ranges) {
        messageLength += 2*sizeof(Hash) + sizeof(uint32_t) + range.owner.size();
    }

    auto responseWriter = [nodeInfo, &ranges](crossbow::buffer_writer& message, std::error_code& /* ec */) {
        // Write host addresses
        message.write<uint32_t>(nodeInfo.size());
        message.write(nodeInfo.data(), nodeInfo.size());

        // Write ranges
        message.write<uint32_t>(ranges.size()); // number of ranges
        for (const auto& range : ranges) {            
            message.write<Hash>(range.start);
            message.write<Hash>(range.end);

            message.write<uint32_t>(range.owner.size());
            message.write(range.owner.data(), range.owner.size());
        }
    };
    con->writeResponse(messageId, ResponseType::CLUSTER_STATE, messageLength, responseWriter);
}

/**
 * @brief Updates the node directory with new status information from a node.
 */
void ServerManager::handleUnregisterNode(ServerSocket *con,
                                         crossbow::infinio::MessageId messageId,
                                         crossbow::buffer_reader &message) {
    uint32_t hostSize = message.read<uint32_t>();
    crossbow::string host(message.read(hostSize), hostSize);

    // Remove the node from the node directory
    mDirectory.erase(host);

    // Remove the node from the hash ring
    mNodeRing.removeNode(host);

    // Write response
    uint32_t messageLength = sizeof(uint8_t);
    auto responseWriter = [](crossbow::buffer_writer& message, std::error_code& /* ec */) { 
        message.write<uint8_t>(0x1u); 
    };
    con->writeResponse(messageId, ResponseType::COMMIT, messageLength, responseWriter);
}

/**
 * @brief Registers a successful key-transfer in the commit-managers hash-ring.
 */
void ServerManager::handleTransferOwnership(ServerSocket *con,
                                            crossbow::infinio::MessageId messageId,
                                            crossbow::buffer_reader &message) {
    uint32_t fromHostSize = message.read<uint32_t>();
    crossbow::string fromHost(message.read(fromHostSize), fromHostSize);

    uint32_t toHostSize = message.read<uint32_t>();
    crossbow::string toHost(message.read(toHostSize), toHostSize);

    // Now we can mark the new node as active for the transferred ranges
    mNodeRing.insertNode(toHost, toHost);
    
    // Write response
    uint32_t messageLength = sizeof(uint8_t);
    auto responseWriter = [](crossbow::buffer_writer& message, std::error_code& /* ec */) { 
        message.write<uint8_t>(0x1u); 
    };
    con->writeResponse(messageId, ResponseType::COMMIT, messageLength, responseWriter);
}

std::vector<crossbow::string> ServerManager::getMatchingHosts(crossbow::string tag) {
    std::vector<crossbow::string> matchingHosts;
    for (const auto& nodeIt : mDirectory) {
        if (mNodeRing.isActive(nodeIt.second->host) && nodeIt.second->tag == tag) {
            matchingHosts.push_back(nodeIt.second->host);
        }
    }
    return std::move(matchingHosts);
}

std::vector<crossbow::string> ServerManager::getBootstrappingHosts(crossbow::string tag) {
    std::vector<crossbow::string> bootstrappingHosts;
    for (const auto& nodeIt : mDirectory) {
        if (mNodeRing.isActive(nodeIt.second->host) && nodeIt.second->tag == tag && nodeIt.second->isBootstrapping) {
            bootstrappingHosts.push_back(nodeIt.second->host);
        }
    }
    return std::move(bootstrappingHosts);
}

} // namespace commitmanager
} // namespace tell
