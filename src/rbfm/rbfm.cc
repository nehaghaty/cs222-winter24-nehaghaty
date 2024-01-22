#include "src/include/rbfm.h"
#include <iostream>

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        return PagedFileManager::instance().createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return PagedFileManager::instance().destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return PagedFileManager::instance().openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        return PagedFileManager::instance().closeFile(fileHandle);
    }

    void createNewPageDir(FileHandle &fileHandle, char *page){
        char *inBuffer = (char*)malloc(PAGE_SIZE);
        char* buf_ptr = inBuffer+PAGE_SIZE-1-4;
        int free = PAGE_SIZE-8;
        memcpy(buf_ptr,&free, sizeof(int));
        buf_ptr-=4;
        int num_records = 0;
        memcpy(buf_ptr,&num_records, sizeof(int));
        fileHandle.appendPage(inBuffer);
        memcpy(page,inBuffer,PAGE_SIZE);
    }

    void getPage(char *page, FileHandle &fileHandle, int record_size) {
        if(fileHandle.getNumberOfPages() == 0){
            createNewPageDir(fileHandle, page);
        }
        else{
            // read page
            char* data = (char*)malloc(PAGE_SIZE);
            fileHandle.readPage(fileHandle.getNumberOfPages(), data);
            char* free_bytes_ptr = data+PAGE_SIZE-1-4;
            int free = *(int*)free_bytes_ptr;
            if(record_size>free){
                createNewPageDir(fileHandle, page);
            }
            else{
                fileHandle.readPage(fileHandle.getNumberOfPages(), page);
            }
        }
    }

    void copyRecordToPageBuf(char* record, int record_length, int seekLen, char* page_ptr){
        printf("record length: %d\n", record_length);
        printf("seek len: %d\n", seekLen);
        memcpy(page_ptr+seekLen,record, record_length);
    }
    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        int number_of_fields = recordDescriptor.size();
        char *data_pointer      =  (char*)data;
        char *record_pointer;
        char *offset_pointer;
        char *record;
        int total_size          =  0;
        int bit_vector_size     = (int)ceil((double)number_of_fields / 8);
        std::vector<bool> isNull;

        //find out which fields are NULL
        for (int i = 0; i < bit_vector_size; i++) {
            for (int bit = 7; bit >= 0; --bit) {
                // Check if the bit is set
                bool isBitSet = (((char*)data)[i] & (1 << bit)) != 0;

                isNull.push_back(isBitSet);
            }
        }

        //find out the total size of the memory that the record is going to take
        total_size += 4; //number of fields
        total_size += bit_vector_size; //bit vector

        for (int i = 0; i < number_of_fields; i++) {
            total_size += 4; //offset
            total_size += recordDescriptor[i].length;
        }

        //start filling the record
        record = (char*)malloc(total_size);
        memset (record, 0, total_size);

        record_pointer = record;
        *((int *)record_pointer) = number_of_fields;
        record_pointer += 4;

        memcpy(record_pointer, data, bit_vector_size);
        record_pointer += bit_vector_size;
        data_pointer += bit_vector_size;

        offset_pointer = record_pointer;
        record_pointer += (sizeof(int) * number_of_fields);

        // creating record from the original data format
        for (int i = 0; i < number_of_fields; i++) {
            if (isNull[i]) {
                int offset = record_pointer - record;
                std::memcpy((int*)offset_pointer, &offset, sizeof (int));
                offset_pointer += 4;
                continue;
            }

            //int data
            if (recordDescriptor[i].type == 0) {

                int int_data;
                memcpy(&int_data, data_pointer, sizeof (int));

                data_pointer += 4;

                memcpy((int*)record_pointer, &int_data, sizeof (int));
                record_pointer += 4;

                int offset = record_pointer - record;
                memcpy((int*)offset_pointer, &offset, sizeof (int));
                offset_pointer +=4 ;
            }

            //float data
            else if (recordDescriptor[i].type == 1) {

                float real_data;
                memcpy(&real_data, data_pointer, sizeof (float));

                data_pointer += 4;

                memcpy((float*)record_pointer, &real_data, sizeof (float));
                record_pointer += 4;

                int offset = record_pointer - record;
                memcpy((int*)offset_pointer, &offset, sizeof (int));
                offset_pointer +=4 ;
            }
            //varchar data
            else if (recordDescriptor[i].type == 2) {
                int size_of_data;

                memcpy(&size_of_data, data_pointer, sizeof (int));

                size_of_data = std::min(size_of_data, (int)recordDescriptor[i].length);

                data_pointer += 4;

                memcpy(record_pointer, data_pointer, size_of_data);
                record_pointer += size_of_data;
                data_pointer += size_of_data;

                int offset = record_pointer - record;
                memcpy((int*)offset_pointer, &offset, sizeof (int));
                offset_pointer +=4 ;
            }
        }

        printf("%d\n", *(int*)(record));
        printf("%d\n", *(int*)(record + 5));
        printf("%d\n", *(int*)(record + 9));
        printf("%d\n", *(int*)(record + 13));
        printf("%d\n", *(int*)(record + 17));

        printf("%c %c\n", *(int*)(record + 21), *(int*)(record + 28));
        printf("%d\n", *(int*)(record + 29));
        printf("%f\n", *(float*)(record + 33));
        printf("%d\n", *(int*)(record + 37));
        printf("total size of records: %d\n", total_size);
//        for (auto i : record_details) {
//            std:: cout << i << std::endl;
//        }
        // write to page:
        // check if record size is greater than PAGE_SIZE: return -1 for now
        if(total_size > PAGE_SIZE){
            return -1;
        }

        // in memory page buffer
        char* page = (char*)malloc(PAGE_SIZE);
        getPage(page, fileHandle, total_size);
        printf("free bytes in new page: %d\n", *(int*)(page+PAGE_SIZE-1-4));
        printf("num records in new page: %d\n", *(int*)(page+PAGE_SIZE-1-8));
        // get num records 'n' from page
        char* page_ptr = page;
        char* slot_ptr = page_ptr+PAGE_SIZE-1-8;
        int num_records = *(int*)(page_ptr+PAGE_SIZE-1-8);
        int seekLen=0;
        if(num_records == 0){
            copyRecordToPageBuf(record, total_size,seekLen,page_ptr);
        }
        else{

            int skipBytes = num_records*4;
            char* slot = slot_ptr-skipBytes;
            short offset = *(short*) slot;
            slot+=2;
            short length = *(short*) slot;
            seekLen = offset+length;
            copyRecordToPageBuf(record, total_size,seekLen, page_ptr);
        }
        num_records++;
        page_ptr = page_ptr+PAGE_SIZE-1-8;
        memcpy(page_ptr, &num_records, sizeof(int));
        page_ptr = page_ptr+4;
        int newFree = PAGE_SIZE - 8 - total_size;
        memcpy(page_ptr,&newFree, sizeof(int));
        slot_ptr = slot_ptr-num_records*4;
//        int newOffset = seekLen+total_size;
        memcpy(slot_ptr,&seekLen, sizeof(short));
        slot_ptr+=2;
        memcpy(slot_ptr,&total_size, sizeof(short));

        fileHandle.writePage(fileHandle.getNumberOfPages(), page);

        //RID
        rid.pageNum = fileHandle.getNumberOfPages();
        rid.slotNum = num_records-1;


        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {

        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

} // namespace PeterDB

