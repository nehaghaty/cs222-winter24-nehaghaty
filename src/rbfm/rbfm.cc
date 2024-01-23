#include "src/include/rbfm.h"
#include <iostream>
#include <cstring>
#include <sstream>

#define FIELD_BYTES        4
#define OFFSET_BYTES        4

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

    static int getActualByteForNullsIndicator(int fieldCount) {

        return ceil((double) fieldCount / CHAR_BIT);
    }

    static int calculateFormattedRecordSize(int nullAttributesIndicatorSize,
                                            const std::vector<Attribute> &recordDescriptor){
        int recordSize = 0;
        recordSize += FIELD_BYTES; //number of fields
        recordSize += nullAttributesIndicatorSize; //null bit vector

        for (int i = 0; i < recordDescriptor.size(); i++) {
            recordSize += OFFSET_BYTES; //offsets
            recordSize += recordDescriptor[i].length;
        }
        return recordSize;
    }
    static void buildRecord(int* recordSize, char*& record, const std::vector<Attribute> &recordDescriptor,
                            const void *data, int nullAttributesIndicatorSize,
                            std::vector<bool> isNull){
        //start filling the record
        int fieldCount = recordDescriptor.size();
        record = (char*)malloc(*recordSize);
        memset (record, 0, *recordSize);

        char *dataPointer = (char*)data;
        char* recordPointer = record;
        *((int *)recordPointer) = fieldCount;
        recordPointer += 4;
        memcpy(recordPointer, data, nullAttributesIndicatorSize);
        recordPointer += nullAttributesIndicatorSize;
        dataPointer += nullAttributesIndicatorSize;

        char* offsetPointer = recordPointer;
        recordPointer += (sizeof(int) * fieldCount);

        // creating record from the original data format
        for (int i = 0; i < fieldCount; i++) {
            if (isNull[i]) {
                int offset = recordPointer - record;
                std::memcpy((int*)offsetPointer, &offset, sizeof (int));
                offsetPointer += 4;
                continue;
            }

            //integer data
            if (recordDescriptor[i].type == PeterDB::TypeInt) {

                int int_data;
                memcpy(&int_data, dataPointer, sizeof (int));

                dataPointer += 4;

                memcpy((int*)recordPointer, &int_data, sizeof (int));
                recordPointer += 4;

                int offset = recordPointer - record;
                memcpy((int*)offsetPointer, &offset, sizeof (int));
                offsetPointer +=4 ;
            }
                //float data
            else if (recordDescriptor[i].type == PeterDB::TypeReal) {

                float real_data;
                memcpy(&real_data, dataPointer, sizeof (float));

                dataPointer += 4;

                memcpy((float*)recordPointer, &real_data, sizeof (float));
                recordPointer += 4;

                int offset = recordPointer - record;
                memcpy((int*)offsetPointer, &offset, sizeof (int));
                offsetPointer +=4 ;
            }
                //varchar data
            else if (recordDescriptor[i].type == PeterDB::TypeVarChar) {
                *recordSize -= recordDescriptor[i].length;
                int dataSize;

                memcpy(&dataSize, dataPointer, sizeof (int));

                dataSize = std::min(dataSize, (int)recordDescriptor[i].length);
                *recordSize += dataSize;

                dataPointer += 4;

                memcpy(recordPointer, dataPointer, dataSize);
                recordPointer += dataSize;
                dataPointer += dataSize;

                int offset = recordPointer - record;
                memcpy((int*)offsetPointer, &offset, sizeof (int));
                offsetPointer +=4 ;
            }
        }
    }

    void createNewPageDir(FileHandle &fileHandle, char* page){
        char *inBuffer = (char*)malloc(PAGE_SIZE);
//        memset(inBuffer,0,PAGE_SIZE);
        char* buf_ptr = inBuffer+PAGE_SIZE-1-4;
        int freeBytes = PAGE_SIZE - 8;
        memcpy(buf_ptr, &freeBytes, sizeof(int));
        buf_ptr-=4;
        int num_records = 0;
        memcpy(buf_ptr,&num_records, sizeof(int));
        fileHandle.appendPage(inBuffer);
        memcpy(page,inBuffer,PAGE_SIZE);
        free(inBuffer);
    }

    void getPage(char *page, FileHandle &fileHandle, int record_size) {
        if(fileHandle.getNumberOfPages() == 0){
            createNewPageDir(fileHandle, page);
        }
        else{
            // read page
            char* data = (char*)malloc(PAGE_SIZE);
            fileHandle.readPage(fileHandle.getNumberOfPages(), data);
            int freeBytes = *(int*)(data + PAGE_SIZE - 1 - 4);
//            int numRecs = *(int*)(data + PAGE_SIZE - 1 - 8);
            if(record_size+4 > freeBytes){
//                bool existingPageFound = false;
//                void* existingPageBuf= malloc(PAGE_SIZE);
//                for(int i=0;i<fileHandle.getNumberOfPages();i++){
//                    fileHandle.readPage(i+1,existingPageBuf);
//                    char* existingPageBufPtr = (char*)existingPageBuf;
//                    int existingPageFreeSpace = *(int*)(existingPageBufPtr+PAGE_SIZE-1-4);
//                    if(existingPageFreeSpace > record_size+4){
//                        existingPageFound = true;
//                        break;
//                    }
//                }
//                if(existingPageFound){
//                    page = (char*)existingPageBuf;
//                }
//                else{
                    createNewPageDir(fileHandle, page);
//                }

                // get free of each page
                // if free > recordsize+ 4
                // add record to that page
                // set pageFound to true
                // break;
//                if(!pageFound){
//                    createNewPageDir(fileHandle, page);
//                }
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
        int fieldCount = recordDescriptor.size();
        char *record = nullptr;

        int nullAttributesIndicatorSize = getActualByteForNullsIndicator(fieldCount);
        std::vector<bool> isNull;

        //find out which fields are NULL
        for (int i = 0; i < nullAttributesIndicatorSize; i++) {
            for (int bit = CHAR_BIT-1; bit >= 0; --bit) {
                // Check if the bit is set
                isNull.push_back((((char*)data)[i] & (1 << bit)) != 0);
            }
        }

        // calculate formatted record size
        int recordSize =  calculateFormattedRecordSize(nullAttributesIndicatorSize,recordDescriptor);

        buildRecord(&recordSize, record, recordDescriptor, data, nullAttributesIndicatorSize, isNull);


        printf("total size of records: %d\n", recordSize);

        // write to page:
        // check if record size is greater than PAGE_SIZE: return -1 for now
        if(recordSize > PAGE_SIZE){
            return -1;
        }

        // in memory page buffer
        char* page = (char*)malloc(PAGE_SIZE);
        getPage(page, fileHandle, recordSize);

        printf("num records in new page: %d\n", *(int*)(page+PAGE_SIZE-1-8));
        // get num records 'n' from page
        char* page_ptr = page;
        char* slot_ptr = page_ptr+PAGE_SIZE-1-8;
        int num_records = *(int*)(page_ptr+PAGE_SIZE-1-8);
        int curr_free = *(int*)(page+PAGE_SIZE-1-4);
        printf("free bytes: %d\n", curr_free);
        int seekLen=0;
        if(num_records == 0){
            copyRecordToPageBuf(record, recordSize, seekLen, page_ptr);
        }
        else{

            int skipBytes = num_records*4;
            char* slot = slot_ptr-skipBytes;
            short offset = *(short*) slot;
            slot+=2;
            short length = *(short*) slot;
            seekLen = offset+length;
            copyRecordToPageBuf(record, recordSize, seekLen, page_ptr);
        }
        // update number of records
        num_records++;
        page_ptr = page_ptr+PAGE_SIZE-1-8;
        memcpy(page_ptr, &num_records, sizeof(int));
        page_ptr = page_ptr+4;
        int newFree = curr_free - recordSize - 4; // reduce free size by slot (4 bytes) and record size
        memcpy(page_ptr,&newFree, sizeof(int));
        slot_ptr = slot_ptr-num_records*4;
        // add offset of new record
        memcpy(slot_ptr,&seekLen, sizeof(short));
        slot_ptr+=2;
        // add length of new record
        memcpy(slot_ptr, &recordSize, sizeof(short));

        fileHandle.writePage(fileHandle.getNumberOfPages(), page);

        //RID
        rid.pageNum = fileHandle.getNumberOfPages();
        rid.slotNum = num_records-1;

        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        int page_num = rid.pageNum;
        int slot_num = rid.slotNum;

        void *page_data = malloc(PAGE_SIZE);
        memset(page_data, 0, PAGE_SIZE);

        if (fileHandle.readPage(page_num, page_data))
            std::cout << "Error reading page" << std::endl;

        char *data_pointer = (char*)page_data;
//        printf("%d\n", *(int*)(data_pointer + PAGE_SIZE - 9));

        short offset = *(short*)(data_pointer + PAGE_SIZE - 1 - 8 - 4 * (slot_num + 1));
        short length = *(short*)(data_pointer + PAGE_SIZE - 1 - 8 - 4 * (slot_num + 1) + 2);

        char *record = (char*) malloc(length);
        memcpy(record, data_pointer + offset, length);
        char *record_pointer = record;

        //deserialize
        int number_of_fields    =  recordDescriptor.size();
        int bit_vector_size     =  (int)ceil((double)number_of_fields / 8);
        std::vector<bool> isNull;

        record_pointer += 4;

        //find out which fields are NULL
        for (int i = 0; i < bit_vector_size; i++) {
            for (int bit = 7; bit >= 0; --bit) {
                // Check if the bit is set
                bool isBitSet = (((char*)record_pointer)[i] & (1 << bit)) != 0;

                isNull.push_back(isBitSet);
            }
        }

        int total_size =  0;

        total_size += bit_vector_size;
        for (int i = 0; i < number_of_fields; i++) {
            total_size += recordDescriptor[i].length;
            if (recordDescriptor[i].type == 2)
                total_size += 4;
        }
        char *data_to_be_returned = (char*) malloc(total_size);
        memset(data_to_be_returned, 0, total_size);

        char *new_data_pointer = data_to_be_returned;
        memcpy(new_data_pointer, record_pointer, bit_vector_size);

        record_pointer += bit_vector_size;
        new_data_pointer += bit_vector_size;

        char *offset_pointer = record_pointer;
        record_pointer += number_of_fields * sizeof (int);

        for (int i = 0; i < number_of_fields; i++) {
            if (isNull[i]) {
                offset_pointer += 4;
                continue;
            }
            if (recordDescriptor[i].type == 0) {
                int int_data;
                memcpy(&int_data, record_pointer, sizeof (int));

                record_pointer += 4;

                memcpy((int*)new_data_pointer, &int_data, sizeof (int));
                new_data_pointer += 4;

                offset_pointer +=4 ;
            }
            else if (recordDescriptor[i].type == 1) {
                int float_data;
                memcpy(&float_data, record_pointer, sizeof (float));

                record_pointer += 4;

                memcpy((int*)new_data_pointer, &float_data, sizeof (float));
                new_data_pointer += 4;

                offset_pointer +=4 ;
            }
            else if (recordDescriptor[i].type == 2) {
                int length_of_string = record + *(int*)offset_pointer - record_pointer;
                memcpy((int*)new_data_pointer, &length_of_string, sizeof (int));
                new_data_pointer += 4;

                memcpy(new_data_pointer, record_pointer, length_of_string);

                new_data_pointer += length_of_string;
                record_pointer += length_of_string;
                offset_pointer += 4;
            }
        }
        memcpy(data, data_to_be_returned, record_pointer - record);
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        std::vector<std::string> record_details;
        char *data_pointer      =  (char*)data;
        int number_of_fields = recordDescriptor.size();

        int bit_vector_size     = (int)ceil((double)number_of_fields / 8);
        std::vector<bool> null_or_not;

        //find out which fields are NULL
        for (int i = 0; i < bit_vector_size; i++) {
            for (int bit = 7; bit >= 0; --bit) {
                // Check if the bit is set
                bool isBitSet = (data_pointer[i] & (1 << bit)) != 0;

                null_or_not.push_back(isBitSet);
            }
        }
        data_pointer += bit_vector_size;
        for (int i = 0; i < number_of_fields; i++) {
            if (null_or_not[i]) {
                record_details.push_back("NULL");
                continue;
            }

            //int data
            if (recordDescriptor[i].type == 0) {

                int int_data;
                memcpy(&int_data, data_pointer, sizeof (int));

                data_pointer += 4;

                record_details.push_back(std::to_string(int_data));
            }

                //float data
            else if (recordDescriptor[i].type == 1) {

                float real_data;
                std::ostringstream ss;
                memcpy(&real_data, data_pointer, sizeof (float));
                ss << real_data;

                data_pointer += 4;
//                std::cout << "insert  " << real_data << std::endl;
                record_details.push_back(ss.str());
            }
                //varchar data
            else if (recordDescriptor[i].type == 2) {
                int size_of_data;

                memcpy(&size_of_data, data_pointer, sizeof (int));

                size_of_data = std::min(size_of_data, (int)recordDescriptor[i].length);
                char *buffer = (char*) malloc(size_of_data);
                data_pointer += 4;

                memcpy(buffer, data_pointer, size_of_data);

                data_pointer += size_of_data;

                record_details.push_back(buffer);
            }
        }
        std::string output_str = "";
        for (int i = 0; i < record_details.size()-1; i ++ ) {
            output_str += recordDescriptor[i].name + ": " + record_details[i] + ", ";
        }

        output_str += recordDescriptor.back().name + ": " + record_details.back();
        out << output_str;
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

