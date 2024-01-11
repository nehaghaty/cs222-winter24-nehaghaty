#include <fstream>
#include <filesystem>
#include <iostream>
#include <sys/stat.h>
#include "src/include/pfm.h"

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    // Check whether the given file exists
    bool fileExists(const std::string &fileName) {
        struct stat stFileInfo{};

        return stat(fileName.c_str(), &stFileInfo) == 0;
    }

    RC PagedFileManager::createFile(const std::string &fileName) {
        if (fileExists(fileName)){
            std::cout << "File already exists" << std::endl;
            return -1;
        }
        else{
            //create file
            std::fstream file(fileName.c_str(), std::ios::out | std::ios::in |
            std::ios::trunc|std::ios::binary);
            if(file){
                std::cout << "File created successfully" << std::endl;
                return 0;
            }
        }
        std::cout << "File creation failed" << std::endl;
        return -1;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        return -1;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        // check if file exists
        if(fileExists(fileName)){
            std::fstream file;
            file.open(fileName.c_str(), std::ios::out|std::ios::in|std::ios::app);
            if (!file) {
                std::cout << "File not opened"<< std::endl;
                return -1;
            }
            else {
                std::cout << "File opened successfully" << std::endl;
                fileHandle = FileHandle();
                return 0;
            }
        }
        else{
            std::cout << "File does not exist" << std::endl;
            return -1;
        }
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        return -1;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        return -1;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        return -1;
    }

    RC FileHandle::appendPage(const void *data) {
        return -1;
    }

    unsigned FileHandle::getNumberOfPages() {
        return -1;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return -1;
    }

} // namespace PeterDB