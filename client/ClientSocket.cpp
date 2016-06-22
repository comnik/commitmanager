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

#include <crossbow/logger.hpp>

namespace tell {
namespace commitmanager {

void StartResponse::processResponse(crossbow::buffer_reader& message) {
    setResult(SnapshotDescriptor::deserialize(message));
}

void CommitResponse::processResponse(crossbow::buffer_reader& message) {
    setResult(message.read<uint8_t>() != 0x0u);
}

void ClusterStateResponse::processResponse(crossbow::buffer_reader &message) {
    std::unique_ptr<ClusterMeta> clusterMeta(new ClusterMeta);

    uint32_t hostsSize = message.read<uint32_t>();
    clusterMeta->hosts = crossbow::string(message.read(hostsSize), hostsSize);
    
    clusterMeta->ranges.emplace_back("localhost:7243", 1, 50);
    
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

std::shared_ptr<ClusterStateResponse> ClientSocket::registerNode(crossbow::infinio::Fiber &fiber,
                                                                     crossbow::string host,
                                                                     crossbow::string tag) {
    auto response = std::make_shared<ClusterStateResponse>(fiber);

    uint32_t messageLength = 2 * sizeof(uint32_t) + host.size() + tag.size();
    auto requestWriter = [host, tag] (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint32_t>(host.size());
        message.write(host.data(), host.size());
        message.write<uint32_t>(tag.size());
        message.write(tag.data(), tag.size());
    };

    sendRequest(response, WrappedResponse::REGISTER_NODE, messageLength, requestWriter);
    return response;
}

std::shared_ptr<CommitResponse> ClientSocket::unregisterNode(
    crossbow::infinio::Fiber &fiber, crossbow::string host) {

    auto response = std::make_shared<CommitResponse>(fiber);

    uint32_t messageLength = sizeof(uint32_t) + host.size();
    auto requestWriter = [host] (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint32_t>(host.size());
        message.write(host.data(), host.size());
    };

    sendRequest(response, WrappedResponse::UNREGISTER_NODE, messageLength, requestWriter);
    return response;
}

} // namespace commitmanager
} // namespace tell
