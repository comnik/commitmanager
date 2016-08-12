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
    // using Hash = size_t;

    /**
     * Describes a partition [start, end] and the node that currently owns it.
     * Used by HashRing clients like the commitmanager server and for serialization.
     */
    struct Partition {
        const crossbow::string owner;
        Hash start;
        Hash end;

        Partition(crossbow::string owner, Hash start, Hash end) 
            : owner(owner),
              start(start),
              end(end) {}
    };

    /**
     * Models a single virtual partition on the hash ring.
     */
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
     * Hash ring data structure that implements the consistent hashing algorithm.
     */
    class HashRing {
        public:
            HashRing(uint32_t numVirtualNodes) 
                : numVirtualNodes(numVirtualNodes) {}

            /** Helper function that converts the 128-bit tokens to crossbow strings. */
            static crossbow::string writeHash(Hash hash);

            /** Computes a 128-bit token for tuples identified by (table id, tuple key). */
            static Hash getPartitionToken(uint64_t tableId, uint64_t key);

            /** Computes a 128-bit token for virtual nodes identified by (physical node name, index). */
            static Hash getPartitionToken(const crossbow::string& nodeName, uint32_t vnode);

            /** Inserts a new physical node into the hash ring. */
            Hash insertNode(const crossbow::string& nodeName);
            
            /** Removes a new physical node into the hash ring. */
            std::vector<Partition> removeNode(const crossbow::string& nodeName);

            /** Removes the bootstrapping tag from an in-flight partition. */
            void transferOwnership(Hash rangeEnd);

            /** Returns the partition a tuple (identified by table id and key) is contained in. */
            const PartitionMeta* getNode(uint64_t tableId, uint64_t key) const;

            /** Returns the partition a given partition token is contained in. */
            const PartitionMeta* getNode(Hash token) const;

            // @TODO: Deprecated, should be removed.
            const std::map<Hash, PartitionMeta>& getRing() const;

            /** Checks wether a given physical node is already contained in the ring. */
            const bool isActive(const crossbow::string& nodeName) const;

            /** Returns all partitions a given physical node is responsible for. */
            std::vector<Partition> getRanges(const crossbow::string& nodeName) const;

            /** Checks wether the hash ring contains any nodes. */
            const bool isEmpty() { return nodeRing.empty(); }

            /** Computes the length of the hash rings serialized representation. */
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

            /** Serializes the hash ring into a buffer. */
            void serialize(crossbow::buffer_writer& writer) const;

            /** Reads and constructs a hash ring from a buffer. */
            static std::unique_ptr<HashRing> deserialize(crossbow::buffer_reader& reader);

        private:
            // Helper method used by getRanges
            void getRange(std::vector<Partition>& ranges, Hash hash) const;
            
            // Murmur seed
            static const uint32_t SEED = 0;

            // The number of tokens per physical node
            const uint32_t numVirtualNodes;

            // A sorted map storing the actual ring elements
            std::map<Hash, PartitionMeta> nodeRing;
    };

} // namespace store
} // namespace tell
