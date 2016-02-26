###################
# CommitManager server
###################
set(SERVER_SRCS
    ServerSocket.cpp
)

set(SERVER_PRIVATE_HDR
    ServerConfig.hpp
    ServerSocket.hpp
        DirectoryEntry.h)

# Add CommitManager server executable
add_executable(commitmanagerd main.cpp ${SERVER_SRCS} ${SERVER_PRIVATE_HDR})
target_include_directories(commitmanagerd PRIVATE ${PROJECT_SOURCE_DIR})

# Link against CommitManager common
target_link_libraries(commitmanagerd PRIVATE commitmanager-common)

# Link against Crossbow
target_include_directories(commitmanagerd PRIVATE ${Crossbow_INCLUDE_DIRS})
target_link_libraries(commitmanagerd PRIVATE crossbow_infinio crossbow_logger)

# Link against Jemalloc
target_include_directories(commitmanagerd PRIVATE ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(commitmanagerd PRIVATE ${Jemalloc_LIBRARIES})

# Install the binary
install(TARGETS commitmanagerd
        RUNTIME DESTINATION ${BIN_INSTALL_DIR})
