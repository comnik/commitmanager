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

#include <commitmanager/ClientSocket.hpp>
#include <commitmanager/MessageTypes.hpp>
#include <commitmanager/HashRing.hpp>

#include <crossbow/logger.hpp>

namespace tell {
namespace commitmanager {

void StartResponse::processResponse(crossbow::buffer_reader& message) {
    std::unique_ptr<ClusterState> clusterState(new ClusterState);

    clusterState->snapshot = std::move(SnapshotDescriptor::deserialize(message));
    clusterState->directoryVersion = message.read<uint64_t>();
    clusterState->numPeers = 0; // will be set later when parsing the peers string

    uint32_t peersSize = message.read<uint32_t>();
    clusterState->peers = crossbow::string(message.read(peersSize), peersSize);
    
    clusterState->hashRing = std::move(HashRing::deserialize(message));

    setResult(std::move(clusterState));
}

void CommitResponse::processResponse(crossbow::buffer_reader& message) {
    setResult(message.read<uint8_t>() != 0x0u);
}

void ClusterStateResponse::processResponse(crossbow::buffer_reader &message) {
    std::unique_ptr<ClusterMeta> clusterMeta(new ClusterMeta);
    
    // Read ranges
    uint32_t numRanges = message.read<uint32_t>();
    for (uint32_t i = 0; i < numRanges; ++i) {
        Hash rangeStart = message.read<Hash>();
        Hash rangeEnd   = message.read<Hash>();

        uint32_t ownerSize = message.read<uint32_t>();

        clusterMeta->ranges.emplace_back(crossbow::string(message.read(ownerSize), ownerSize), rangeStart, rangeEnd);
    }
    
    setResult(std::move(clusterMeta));
}

void ClientSocket::connect(const crossbow::infinio::Endpoint& host) {
    LOG_INFO("Connecting to CommitManager server %1%", host);

    crossbow::infinio::RpcClientSocket::connect(host, handshakeString());
}

void ClientSocket::shutdown() {
    LOG_INFO("Shutting down CommitManager connection");

    crossbow::infinio::RpcClientSocket::shutdown();
}

std::shared_ptr<StartResponse> ClientSocket::startTransaction(crossbow::infinio::Fiber& fiber, bool readonly) {
    auto response = std::make_shared<StartResponse>(fiber);

    uint32_t messageLength = sizeof(uint8_t);
    sendRequest(response, WrappedResponse::START, messageLength, [readonly] (crossbow::buffer_writer& message,
                                                                             std::error_code& /* ec */) {
        message.write<uint8_t>(readonly ? 0x1u : 0x0u);
    });

    return response;
}

std::shared_ptr<CommitResponse> ClientSocket::commitTransaction(crossbow::infinio::Fiber& fiber, uint64_t version) {
    auto response = std::make_shared<CommitResponse>(fiber);

    uint32_t messageLength = sizeof(uint64_t);
    sendRequest(response, WrappedResponse::COMMIT, messageLength, [version] (crossbow::buffer_writer& message,
                                                                             std::error_code& /* ec */) {
        message.write<uint64_t>(version);
    });

    return response;
}

std::shared_ptr<ClusterStateResponse> ClientSocket::registerNode(crossbow::infinio::Fiber& fiber,
                                                                 const SnapshotDescriptor& snapshot,
                                                                 crossbow::string host,
                                                                 crossbow::string tag) {
    auto response = std::make_shared<ClusterStateResponse>(fiber);

    uint32_t messageLength = sizeof(uint64_t) + 2*sizeof(uint32_t) + host.size() + tag.size();
    auto requestWriter = [&snapshot, &host, &tag] (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(snapshot.version());

        message.write<uint32_t>(host.size());
        message.write(host.data(), host.size());

        message.write<uint32_t>(tag.size());
        message.write(tag.data(), tag.size());
    };

    sendRequest(response, WrappedResponse::REGISTER_NODE, messageLength, requestWriter);
    return response;
}

std::shared_ptr<ClusterStateResponse> ClientSocket::unregisterNode(crossbow::infinio::Fiber &fiber, 
                                                                   const SnapshotDescriptor& snapshot,
                                                                   crossbow::string host) {
    auto response = std::make_shared<ClusterStateResponse>(fiber);

    uint32_t messageLength = sizeof(uint64_t) + sizeof(uint32_t) + host.size();
    auto requestWriter = [&snapshot, &host] (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(snapshot.version());

        message.write<uint32_t>(host.size());
        message.write(host.data(), host.size());
    };

    sendRequest(response, WrappedResponse::UNREGISTER_NODE, messageLength, requestWriter);
    return response;
}

std::shared_ptr<CommitResponse> ClientSocket::transferOwnership(crossbow::infinio::Fiber &fiber,
                                                                Hash rangeEnd,
                                                                crossbow::string host) {
    auto response = std::make_shared<CommitResponse>(fiber);

    uint32_t messageLength = sizeof(uint32_t) + sizeof(Hash) + host.size();
    
    auto requestWriter = [&rangeEnd, &host](crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<Hash>(rangeEnd);

        message.write<uint32_t>(host.size());
        message.write(host.data(), host.size());
    };

    sendRequest(response, WrappedResponse::TRANSFER_OWNERSHIP, messageLength, requestWriter);
    return response;
}

} // namespace commitmanager
} // namespace tell
