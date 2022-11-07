# C++ Clickhouse Client Example

## Build Instructions

### Prerequisites

- cmake
- llvm

### Checkout Instructions

Clone recursively to get submodules as well
```
git clone --recursive <repo-url>
```
If the repository has been cloned without submodules, to download them you need to run the following:
```
git submodule init
git submodule update
```

### Build

```bash
mkdir build
export PATH=$(brew --prefix llvm)/bin:$PATH
cmake -S . -B build
cmake --build build
```


