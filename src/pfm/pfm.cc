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

    static long getFileSize(FILE *file) {
        long size;
        fseek(file, 0, SEEK_END);

        size = ftell(file);

        return size;
    }

    RC PagedFileManager::createFile(const std::string &fileName) {
        if (fileExists(fileName)){
            std::cout << "File already exists" << std::endl;
            return -1;
        }
        else{
            //create file
            FILE *file = fopen(fileName.c_str(), "wb");
            if(file){
                //std::cout << "File created successfully" << std::endl;
                fclose(file);
                return 0;
            }
        }
        std::cout << "File creation failed" << std::endl;
        return -1;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        if (fileExists(fileName.c_str())){
            //destroy file
            std::remove (fileName.c_str());
            return 0;
        }
        else {
            return -1;
        }
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        // check if file exists
        if (fileExists(fileName)){
            FILE *file;
            file = fopen(fileName.c_str(), "r+b");
            if (!file) {
                std::cout << "File not opened" << std::endl;
                return -1;
            }
            else {
                //std::cout << "File opened successfully" << std::endl;
                if(fileHandle.opened)
                {
                    std::cout<<"File handle in use!" << std::endl;
                    return -1;
                }
                else {

                    fileHandle.opened=true;
                    fileHandle.file = file;
                    //check for hidden page, create only if not present already
                    if (getFileSize(file) == 0) {
                        void *hiddenPage = malloc(PAGE_SIZE);
                        memset(hiddenPage, 0, PAGE_SIZE);
                        std::fwrite(hiddenPage, PAGE_SIZE, 1, file);
                        free(hiddenPage);
                    } else {
                        fseek(file, 0, SEEK_SET);
                        fread(&fileHandle.totalPages, sizeof (int), 1, file);
                        //std::cout << "Total Pages: " <<fileHandle.totalPages << std::endl;
                    }
//                    std::cout<<"Hidden file created!" << std::endl;
                    return 0;
                }

            }
        }
        else {
            std::cout << "File does not exist" << std::endl;
            return -1;
        }
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        if (fileHandle.opened) {
            fileHandle.opened = false;
            std::fseek(fileHandle.file, 0, SEEK_SET);
            std::fwrite(&fileHandle.totalPages, sizeof (int), 1, fileHandle.file);
            fclose(fileHandle.file);
            return 0;
        }
        else {
            return -1;
        }
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        totalPages = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (pageNum <= totalPages) {
            fseek(file, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
            fread(data, PAGE_SIZE, 1, file);
            readPageCounter++;
            return 0;
        }
        else {
            return -1;
        }
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {

        if (pageNum > totalPages) {
            return -1;
        }

        if (!fseek(file, (pageNum + 1) * PAGE_SIZE, SEEK_SET)) {
            fwrite(data, PAGE_SIZE, 1, file);
            writePageCounter+=1;
            return 0;
        }
        else {
            std::cout << "Error writing data to particular page" << std::endl;
            return -1;
        }
    }

    RC FileHandle::appendPage(const void *data) {
        fseek(file, (totalPages + 1) * PAGE_SIZE, SEEK_SET);
        fwrite(data, PAGE_SIZE, 1, file);
        appendPageCounter++;
        totalPages++;
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return totalPages;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;

        return 0;
    }

} // namespace PeterDB