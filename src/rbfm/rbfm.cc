#include "src/include/rbfm.h"
#include <iostream>
#include <cstring>
#include <sstream>
#include <cmath>

// record constants
typedef unsigned OffsetLength;
typedef unsigned NumFieldsLength;
//page constants
typedef unsigned short SlotSubFieldLength;
typedef unsigned SlotLength;
typedef unsigned NumRecordsLength;
typedef unsigned FreeSpaceLength;
// raw data constants
typedef unsigned VarCharLength;

#define CHAR_BIT           8
#define NUM_RECORDS_OFFSET (PAGE_SIZE-sizeof(FreeSpaceLength)-sizeof(NumRecordsLength)-1)
#define FREE_SPACE_OFFSET (PAGE_SIZE-sizeof(FreeSpaceLength)-1)

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
        recordSize += sizeof(NumFieldsLength); //number of fields
        recordSize += nullAttributesIndicatorSize; //null bit vector

        for (int i = 0; i < recordDescriptor.size(); i++) {
            recordSize += sizeof(OffsetLength); //offsets
            recordSize += recordDescriptor[i].length;
        }
        return recordSize;
    }
    static void buildRecord(int* recordSize, char*& record, const std::vector<Attribute> &recordDescriptor,
                            const void *data, int nullAttributesIndicatorSize,
                            std::vector<bool> isNull){
        //start filling the record
        int fieldCount = recordDescriptor.size();
        record = new char[*recordSize];
        memset (record, 0, *recordSize);

        const char *dataPointer = static_cast<const char*>(data);
        char* recordPointer = record;
        memcpy(recordPointer, &fieldCount, sizeof(int));
        recordPointer += sizeof(NumFieldsLength);
        memcpy(recordPointer, dataPointer, nullAttributesIndicatorSize);
        recordPointer += nullAttributesIndicatorSize;
        dataPointer += nullAttributesIndicatorSize;

        char* offsetPointer = recordPointer;
        recordPointer += sizeof(OffsetLength) * fieldCount;

        // creating record from the original data format
        for (int i = 0; i < fieldCount; i++) {
            if (isNull[i]) {
                int offset = recordPointer - record;
                std::memcpy((int*)offsetPointer, &offset, sizeof (int));
                offsetPointer += sizeof(OffsetLength);
                continue;
            }

            switch (recordDescriptor[i].type) {
                case PeterDB::TypeInt:
                case PeterDB::TypeReal: {
                    memcpy(recordPointer, dataPointer, sizeof(int));
                    recordPointer += sizeof(int);
                    dataPointer += sizeof(int);
                    break;
                }
                case PeterDB::TypeVarChar: {
                    unsigned dataSize;
                    memcpy(&dataSize, dataPointer, sizeof(int));
                    dataSize = std::min(dataSize, recordDescriptor[i].length);
                    *recordSize += dataSize - recordDescriptor[i].length;
                    dataPointer += sizeof(int);
                    memcpy(recordPointer, dataPointer, dataSize);
                    recordPointer += dataSize;
                    dataPointer += dataSize;
                    break;
                }
            }

            int recordOffset = recordPointer - record;
            memcpy(offsetPointer, &recordOffset, sizeof(OffsetLength));
            offsetPointer += sizeof(OffsetLength);
        }
    }

    void createNewPageDir(FileHandle &fileHandle, char* page){
        char *inBuffer = (char*)malloc(PAGE_SIZE);
        memset(inBuffer,0,PAGE_SIZE);
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

    int getPage(char *page, FileHandle &fileHandle, int record_size) {
        if(fileHandle.getNumberOfPages() == 0){
            createNewPageDir(fileHandle, page);
            return fileHandle.getNumberOfPages()-1;
        }
        else{
            // read page
            char* data = (char*)malloc(PAGE_SIZE);
            fileHandle.readPage(fileHandle.getNumberOfPages()-1, data);
            int freeBytes = *(int*)(data + PAGE_SIZE - 1 - 4);
            free(data);

            if(record_size+4 > freeBytes){
                bool existingPageFound = false;
                int i=0;
                void* existingPageBuf= malloc(PAGE_SIZE);
                for(;i<fileHandle.getNumberOfPages();i++){
                    fileHandle.readPage(i,existingPageBuf);
                    char* existingPageBufPtr = (char*)existingPageBuf;
                    int existingPageFreeSpace = *(int*)(existingPageBufPtr+PAGE_SIZE-1-4);
                    if(existingPageFreeSpace > record_size+4){
                        existingPageFound = true;
                        break;
                    }
                }
                if(existingPageFound){
                    memcpy(page, existingPageBuf, PAGE_SIZE);
                    free(existingPageBuf);
                    return i;
                }
                else{
                    createNewPageDir(fileHandle, page);
                    free(existingPageBuf);
                    return fileHandle.getNumberOfPages()-1;
                }
            }
            else{
                fileHandle.readPage(fileHandle.getNumberOfPages()-1, page);
                return fileHandle.getNumberOfPages()-1;
            }
        }
    }

    void copyRecordToPageBuf(const char* record, int record_length, int seekLen, char* page_ptr){
        memcpy(page_ptr+seekLen,record, record_length);
    }

    void encodeBytes(bool isTombstone, unsigned char& buffer) {
    if (isTombstone) {
        buffer |= (1 << 7); // Set first bit to 1
    } else {
        buffer &= ~(1 << 7); // Set first bit to zero
    }
    }

    bool decodeBytes(const unsigned char& buffer) {
    return (buffer & (1 << 7)) != 0; // Checks if the first bit is 1
}
   
    void writeRecordToPage(const char* record, int recordSize, FileHandle &fileHandle, RID &rid) {
        char *page = new char[PAGE_SIZE];
        int pageNum = getPage(page, fileHandle, recordSize);

        char *pagePtr = page;
        char *slotPtr = pagePtr + NUM_RECORDS_OFFSET;
        int numRecords = *(int *)(pagePtr + NUM_RECORDS_OFFSET);
        int currFree = *(int *)(page + FREE_SPACE_OFFSET);
        int seekLen = 0;

        if (numRecords == 0) {
            copyRecordToPageBuf(record, recordSize, seekLen, pagePtr);
        } else {
            int skipBytes = numRecords * sizeof(SlotLength);
            char *slot = slotPtr - skipBytes;
            short offset = *(short *)slot;
            slot += sizeof(SlotSubFieldLength);
            short recordLength = *(short *)slot;
            seekLen = offset + recordLength;
            copyRecordToPageBuf(record, recordSize, seekLen, pagePtr);
        }

        // increment number of records
        numRecords += 1;
        *(int *)(page + NUM_RECORDS_OFFSET) = numRecords;

        // calculate new free space
        int newFree = currFree - recordSize - sizeof(SlotLength);
        *(int *)(page + FREE_SPACE_OFFSET) = newFree;

        // update slot info for the new record
        slotPtr = page + NUM_RECORDS_OFFSET - numRecords * sizeof(SlotLength);
        *(short *)slotPtr = seekLen;
        *(short *)(slotPtr + sizeof(SlotSubFieldLength)) = recordSize;

        fileHandle.writePage(pageNum, page);
        delete[] page;

        //RID
        rid.pageNum = (unsigned)pageNum;
        rid.slotNum = (unsigned short)numRecords-1;
    }
    
    void processBitVector(std::vector<bool>& nullBitVector, int nullAttributesIndicatorSize, const void* bitArrayPointer) {
            for (int i = 0; i < nullAttributesIndicatorSize; i++) {
                for (int bit = CHAR_BIT-1; bit >= 0; --bit) {
                // Check if the bit is set
                nullBitVector.push_back((((char*)bitArrayPointer)[i] & (1 << bit)) != 0);
            }
        }
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        int fieldCount = recordDescriptor.size();
        int nullAttributesIndicatorSize = getActualByteForNullsIndicator(fieldCount);
        std::vector<bool> isNull;

        // Set bits for NULL fields
        processBitVector(isNull, nullAttributesIndicatorSize, data);

        // Calculate formatted record size
        int recordSize =  calculateFormattedRecordSize(nullAttributesIndicatorSize,recordDescriptor);
        char *record = nullptr;

        buildRecord(&recordSize, record, recordDescriptor, data, nullAttributesIndicatorSize, isNull);

        if (recordSize > PAGE_SIZE) {
        delete[] record;
        return -1;
        }

        writeRecordToPage(record, recordSize,fileHandle, rid);

        delete[] record;

        return 0;
    }

    std::pair<short, short> readSlotInformation(const char* dataPointer, int slotNum) {
        short slotOffset = *(short*)(dataPointer + NUM_RECORDS_OFFSET - sizeof(SlotLength) * (slotNum + 1));
        short length = *(short*)(dataPointer + NUM_RECORDS_OFFSET - sizeof(SlotLength) * (slotNum + 1) + sizeof(SlotSubFieldLength));
        return {slotOffset, length};
    }
    
    int getDeserializedRecordSize(const std::vector<Attribute>& recordDescriptor, int nullAttributesIndicatorSize) {
        
        int deserializedRecordSize = nullAttributesIndicatorSize;
        
        for (const auto& attr : recordDescriptor) {
            deserializedRecordSize += attr.length;
            if (attr.type == PeterDB::TypeVarChar) {
                deserializedRecordSize += sizeof(VarCharLength);
            }
        }
        return deserializedRecordSize;
    
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        int pageNum = rid.pageNum;
        int slotNum = rid.slotNum;

        // void *pageData = malloc(PAGE_SIZE);
        char* pageData = new char[PAGE_SIZE]{};
        // memset(pageData, 0, PAGE_SIZE);

        if (fileHandle.readPage(pageNum, pageData)){
            std::cout << "Error reading page" << std::endl;
            return -1;
            }

        char *dataPointer = (char*)pageData;

        auto [slotOffset, length] = readSlotInformation(pageData, slotNum);

        char* record = new char[length];
        memcpy(record, pageData + slotOffset, length);
        delete[] pageData;
        // char *record = (char*) malloc(length);
        // memcpy(record, dataPointer + slotOffset, length);
 

        int numFields  =  recordDescriptor.size();
        int nullAttributesIndicatorSize =  getActualByteForNullsIndicator(numFields);
        
        // Set bits for NULL fields
        std::vector<bool> isNull;
        processBitVector(isNull, nullAttributesIndicatorSize, record + sizeof(NumFieldsLength));

        //deserialize
        int deserializedRecordSize = getDeserializedRecordSize(recordDescriptor, nullAttributesIndicatorSize);

        
        char* deserializedRecordBuf = new char[deserializedRecordSize]{};
        char* deserializedRecordBufPtr = deserializedRecordBuf;

        // copy record to deserialized record
        char* recordPointer = record + sizeof(NumFieldsLength);
        memcpy(deserializedRecordBufPtr, recordPointer, nullAttributesIndicatorSize);

        recordPointer += nullAttributesIndicatorSize;
        deserializedRecordBufPtr += nullAttributesIndicatorSize;

        char *recordOffsetPointer = recordPointer;
        recordPointer += numFields * sizeof(OffsetLength);

        for (int i = 0; i < numFields; i++) {
            if (isNull[i]) {
                recordOffsetPointer += sizeof(OffsetLength);
                continue;
            }
            switch (recordDescriptor[i].type) {
                case PeterDB::TypeInt: // int
                case PeterDB::TypeReal: { // float
                    // Since the size of int and float is the same, we can handle them in the same case
                    memcpy(deserializedRecordBufPtr, recordPointer, sizeof(int));
                    recordPointer += sizeof(int);
                    deserializedRecordBufPtr += sizeof(int);
                    recordOffsetPointer += sizeof(int);
                    break;
                }
                case PeterDB::TypeVarChar: { // string
                    int stringLen = record + *(int*)recordOffsetPointer - recordPointer;
                    memcpy((int*)deserializedRecordBufPtr, &stringLen, sizeof (VarCharLength));
                    deserializedRecordBufPtr += sizeof(VarCharLength);

                    memcpy(deserializedRecordBufPtr, recordPointer, stringLen);

                    deserializedRecordBufPtr += stringLen;
                    recordPointer += stringLen;
                    recordOffsetPointer += sizeof(OffsetLength);
                    break;
                }
            }
        }
        
        memcpy(data, deserializedRecordBuf, deserializedRecordSize);
        delete[] record;
        delete[] deserializedRecordBuf;
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

                record_details.push_back(ss.str());
            }
                //varchar data
            else if (recordDescriptor[i].type == 2) {
                int size_of_data;

                memcpy(&size_of_data, data_pointer, sizeof (int));
                data_pointer += 4;

                if (size_of_data == 0) {
                    record_details.push_back("");
                }
                else {
                    size_of_data = std::min(size_of_data, (int)recordDescriptor[i].length);
                    char* buffer = (char*) malloc(size_of_data+1);
                    buffer[size_of_data]=0;
                    memcpy(buffer, data_pointer, size_of_data);

                    data_pointer += size_of_data;

                    record_details.push_back(buffer);
                    free(buffer);
                }
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

