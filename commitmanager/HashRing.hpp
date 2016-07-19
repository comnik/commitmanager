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
 *     Nikolas Göbel <ngoebel@student.ethz.ch>
 */
#pragma once

#include <map>

#include <crossbow/byte_buffer.hpp>
#include <crossbow/string.hpp>
#include <crossbow/logger.hpp>

#include "MurmurHash3.h"

namespace tell {
namespace commitmanager {

    using Hash = unsigned __int128;

    // Describes a partition [start, end] and the node that currently owns it
    struct Partition {
        const crossbow::string owner;
        Hash start;
        Hash end;

        Partition(crossbow::string owner, Hash start, Hash end) 
            : owner(owner),
              start(start),
              end(end) {}
    };

    struct PartitionMeta {
        crossbow::string owner;
        crossbow::string previousOwner;
        bool isBootstrapping;

        PartitionMeta(const crossbow::string& owner)
            : PartitionMeta(owner, true) {}

        PartitionMeta(const crossbow::string& owner, bool isBootstrapping)
            : owner(owner),
              previousOwner(""),
              isBootstrapping(isBootstrapping) {}

        PartitionMeta(const crossbow::string& owner, const crossbow::string& previousOwner, bool isBootstrapping)
            : owner(owner),
              previousOwner(previousOwner),
              isBootstrapping(isBootstrapping) {}

        PartitionMeta(const PartitionMeta& other)
            : owner(other.owner),
              previousOwner(other.previousOwner),
              isBootstrapping(other.isBootstrapping) {}
    };

    /**
     * @brief Implementation of consistent hashing.
     */
    class HashRing {
        public:
            HashRing(uint32_t numVirtualNodes) 
                : numVirtualNodes(numVirtualNodes) {}

            static crossbow::string writeHash(Hash hash);

            static bool isSubPartition(Hash parentStart, Hash parentEnd, Hash childStart, Hash childEnd); 
            
            static bool inPartition(Hash token, Hash rangeStart, Hash rangeEnd); 

            static Hash getPartitionToken(uint64_t tableId, uint64_t key);
            static Hash getPartitionToken(const crossbow::string& nodeName, uint32_t vnode);

            Hash insertNode(const crossbow::string& nodeName);
            
            std::vector<Partition> removeNode(const crossbow::string& nodeName);

            const PartitionMeta* getNode(uint64_t tableId, uint64_t key) const;
            const PartitionMeta* getNode(Hash token) const;

            const std::map<Hash, PartitionMeta>& getRing() const;

            const bool isActive(const crossbow::string& nodeName) const;

            void getRange(std::vector<Partition>& ranges, Hash hash) const;
            std::vector<Partition> getRanges(const crossbow::string& nodeName) const;

            const bool isEmpty() { return nodeRing.empty(); }

            uint32_t serializedLength() const {
                uint32_t length = 2 * sizeof(uint32_t); // nodeRing.size() + numVirtualNodes

                for (const auto& partitionIt : nodeRing) {
                    length += sizeof(Hash)              // partition id
                            + sizeof(uint32_t)          // owner length
                            + partitionIt.second.owner.size()
                            + sizeof(uint32_t)          // previous owner length
                            + partitionIt.second.previousOwner.size()
                            + sizeof(bool);             // is bootstrapping?
                }

                return length;
            }

            void serialize(crossbow::buffer_writer& writer) const;
            static std::unique_ptr<HashRing> deserialize(crossbow::buffer_reader& reader);

        private:
            // Murmur seed
            static const uint32_t SEED = 0;

            const uint32_t numVirtualNodes;
            std::map<Hash, PartitionMeta> nodeRing;
    };

} // namespace store
} // namespace tell
