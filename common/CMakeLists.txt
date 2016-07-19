###################
# CommitManager common
###################
set(COMMON_SRCS
    CommitManager.cpp
    Descriptor.cpp
    MessageTypes.cpp
    HashRing.cpp
    SnapshotDescriptor.cpp
    MurmurHash3.cpp
)

set(COMMON_PUBLIC_HDR
    CommitManager.hpp
    Descriptor.hpp
    MessageTypes.hpp
    SnapshotDescriptor.hpp
    HashRing.hpp
    MurmurHash3.h
)

# Transform public header list to use absolute paths
foreach(hdr ${COMMON_PUBLIC_HDR})
    list(APPEND COMMON_PUBLIC_HDR_ABSOLUTE ${PROJECT_SOURCE_DIR}/commitmanager/${hdr})
endforeach()

# Add CommitManager common library
add_library(commitmanager-common STATIC ${COMMON_SRCS} ${COMMON_PUBLIC_HDR_ABSOLUTE})
target_include_directories(commitmanager-common PUBLIC ${PROJECT_SOURCE_DIR})
set_target_properties(commitmanager-common PROPERTIES PUBLIC_HEADER "${COMMON_PUBLIC_HDR_ABSOLUTE}")

# Link against Crossbow
target_include_directories(commitmanager-common PRIVATE ${Crossbow_INCLUDE_DIRS})
target_link_libraries(commitmanager-common PRIVATE crossbow_logger)
