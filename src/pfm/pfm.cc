#include <fstream>
#include <filesystem>
#include <iostream>
#include "src/include/pfm.h"
using namespace std;

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        if (filesystem::exists(fileName.c_str())){
            cout << "file already exists" << endl;
            return -1;
        }
        else{
            //create file
            std::fstream file(fileName.c_str(), std::ios::out | std::ios::in |
            std::ios::trunc|std::ios::binary);
            if (filesystem::exists(fileName.c_str())){
                cout << "file created successfully" << endl;
                return 0;
            }
        }
        cout << "file creation failed" << endl;
        return -1;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        return -1;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return -1;
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