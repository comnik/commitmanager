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
          mMaxBatchSize(config.maxBatchSize) {
}

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

    case crossbow::to_underlying(RequestType::START): {
        handleStartTransaction(con, messageId, message);
    } break;

    case crossbow::to_underlying(RequestType::COMMIT): {
        handleCommitTransaction(con, messageId, message);
    } break;

    case crossbow::to_underlying(RequestType::READ_CLUSTER): {
        handleGetClusterState(con, messageId, message);
    } break;

    case crossbow::to_underlying(RequestType::UPDATE_CLUSTER): {
        handleUpdateClusterState(con, messageId, message);
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

void ServerManager::handleStartTransaction(ServerSocket* con, crossbow::infinio::MessageId messageId,
        crossbow::buffer_reader& message) {
    auto readonly = (message.read<uint8_t>() != 0x0u);
    if (!mCommitManager.startTransaction(readonly)) {
        con->writeErrorResponse(messageId, error::transaction_limit_reached);
        return;
    }

    auto messageLength = mCommitManager.serializedLength();
    con->writeResponse(messageId, ResponseType::START, messageLength, [this]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        mCommitManager.serializeSnapshot(message);
    });
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
    void ServerManager::handleUpdateClusterState(ServerSocket *con, crossbow::infinio::MessageId messageId,
                                                 crossbow::buffer_reader &message) {
        // TODO: Read the actual message
        crossbow::string msgType = "REGISTER";
        crossbow::string msgHost = "localhost";
        uint16_t msgPort = 8000;
        crossbow::string msgTag = "STORAGE";

        // Update cluster state
        if (msgType == "REGISTER") {
            DirectoryEntry node;
            node.host = msgHost;
            node.port = msgPort;
            node.tag = msgTag;

            mDirectory.push_back(node);
        } else {
            // TODO handle unregister
        }

        // Write response
        uint64_t messageLength = 0;
        auto responseWriter = [](crossbow::buffer_writer& message, std::error_code& /* ec */) { };
        con->writeResponse(messageId, ResponseType::CLUSTER_STATE, messageLength, responseWriter);
    }

    /**
     * @brief Filters the node directory by the requested tag and returns address info back to the client.
     */
    void ServerManager::handleGetClusterState(ServerSocket *con, crossbow::infinio::MessageId messageId,
                                              crossbow::buffer_reader &message) {

        // TODO: Read the requested tag
        crossbow::string requestedTag = "STORAGE";

        // Filter
        auto tagFilter = [requestedTag](struct DirectoryEntry& entry) { return entry.tag == requestedTag; };
        std::vector<struct DirectoryEntry> matchingEntries;
        std::copy_if(mDirectory.begin(), mDirectory.end(), std::back_inserter(matchingEntries), tagFilter);

        // Construct address info
        auto getHost = [](struct DirectoryEntry& entry) { return entry.host; };
        std::vector<crossbow::string> matchingHosts;
        std::transform(matchingEntries.begin(), matchingEntries.end(), std::back_inserter(matchingHosts), getHost);
        crossbow::string nodeInfo = boost::algorithm::join(matchingHosts, ";");

        // Write response
        uint64_t messageLength = nodeInfo.length();
        auto responseWriter = [nodeInfo](crossbow::buffer_writer& message, std::error_code& /* ec */) {
            message.write(nodeInfo);
        };
        con->writeResponse(messageId, ResponseType::CLUSTER_STATE, messageLength, responseWriter);
    }

} // namespace commitmanager
} // namespace tell
