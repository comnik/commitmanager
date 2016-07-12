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

#pragma once

#include <cstdint>
#include <vector>

#include <crossbow/string.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

namespace tell {
namespace commitmanager {

/**
 * @brief Unique string sent as first argument in the connection handshake
 */
const crossbow::string& handshakeString();

/**
 * @brief The possible messages types of a request
 */
enum class WrappedResponse : uint32_t {
    START = 0x1u,
    COMMIT,
    REGISTER_NODE,
    UNREGISTER_NODE,
    TRANSFER_OWNERSHIP
};

/**
 * @brief The possible messages types of a response
 */
enum class ResponseType : uint32_t {
    START = 0x1u,
    COMMIT,
    CLUSTER_STATE
};

using Hash = unsigned __int128;

// Describes a partition [start, end] and the node that currently owns it
struct Partition {
    const crossbow::string owner;
    Hash start;
    Hash end;

    Partition(crossbow::string owner, Hash start, Hash end) :
                owner(owner),
                start(start),
                end(end)
                {}
};

struct ClusterMeta {
    // Other hosts in the cluster
    crossbow::string hosts;
    // Ranges the recipient is responsible for
    std::vector<Partition> ranges;
};

struct ClusterState {
    size_t numPeers;
    uint64_t directoryVersion;
    crossbow::string peers;
    crossbow::string bootstrappingPeers;
    std::unique_ptr<SnapshotDescriptor> snapshot;
};

} // namespace commitmanager
} // namespace tell
