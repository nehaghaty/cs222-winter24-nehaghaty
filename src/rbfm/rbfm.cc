#include "src/include/rbfm.h"
#include <iostream>
#include <sstream>

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

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        int number_of_fields = recordDescriptor.size();

        char *data_pointer      =  (char*)data;
        char *record_pointer;
        char *offset_pointer;
        char *record;

        int total_size          =  0;

        int bit_vector_size     = (int)ceil((double)number_of_fields / 8);
        std::vector<bool> null_or_not;

        //find out which fields are NULL
        for (int i = 0; i < bit_vector_size; i++) {
            for (int bit = 7; bit >= 0; --bit) {
                // Check if the bit is set
                bool isBitSet = (((char*)data)[i] & (1 << bit)) != 0;

                null_or_not.push_back(isBitSet);
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
            if (null_or_not[i]) {
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
//        for (auto i : record_details) {
//            std:: cout << i << std::endl;
//        }

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
                std::cout << "insert  " << real_data << std::endl;
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
        std::cout << output_str;
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

