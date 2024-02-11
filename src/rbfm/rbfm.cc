#include "src/include/rbfm.h"
#include <iostream>
#include <cstring>
#include <sstream>
#include <cmath>

// record constants
typedef unsigned OffsetLength;
typedef unsigned NumFieldsLength;
typedef unsigned char TombstoneByte;
//page constants
typedef unsigned short SlotSubFieldLength;
typedef unsigned SlotLength;
typedef unsigned NumSlotsLength;
typedef unsigned FreeSpaceLength;
// raw data constants
typedef unsigned VarCharLength;

#define CHAR_BIT    8
#define TOMBSTONE_SIZE  7
#define SLOT_SIZE sizeof(SlotLength)
#define NUM_SLOTS_OFFSET (PAGE_SIZE-sizeof(FreeSpaceLength)-sizeof(NumSlotsLength)-1)
#define FREE_SPACE_OFFSET (PAGE_SIZE-sizeof(FreeSpaceLength)-1)
#define FREE_SPACE_BYTES   sizeof(FreeSpaceLength)
#define NUM_SLOTS_BYTES    sizeof(NumSlotsLength )

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

    static void encodeBytes( bool isTombstone, char& tombstoneByte) {
        if (isTombstone)
            tombstoneByte =1;
        else
            tombstoneByte =0;
    }

    bool decodeBytes(const unsigned char& tombstoneByte) {
        return tombstoneByte != 0;
    }

    static int getActualByteForNullsIndicator(unsigned fieldCount) {

        return ceil((double) fieldCount / CHAR_BIT);
    }

    static unsigned calculateFormattedRecordSize(int nullAttributesIndicatorSize,
                                                 const std::vector<Attribute> &recordDescriptor){
        unsigned recordSize = sizeof(TombstoneByte) + sizeof(NumFieldsLength) + nullAttributesIndicatorSize; //null bit vector

        for (const auto & i : recordDescriptor) {
            recordSize += sizeof(OffsetLength); //offsets
            recordSize += i.length;
        }
        return recordSize;
    }

    static void buildRecord(unsigned* recordSize, char*& record, const std::vector<Attribute> &recordDescriptor,
                            const void *data, int nullAttributesIndicatorSize,
                            std::vector<bool> isNull){
        //start filling the record
        record = new char[*recordSize];
        memset (record, 0, *recordSize);

        const char *dataPointer = static_cast<const char*>(data);
        char* recordPointer = record;

        // fill tombstone byte
        char tombStoneBuff = 0;
        encodeBytes(false, tombStoneBuff);
        memcpy(recordPointer, &tombStoneBuff, sizeof(TombstoneByte));
        recordPointer += sizeof(TombstoneByte);
        // fill num fields
        unsigned fieldCount = recordDescriptor.size();
        memcpy(recordPointer, &fieldCount, sizeof(int));
        recordPointer += sizeof(NumFieldsLength);
        // fill null bit vector
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
        char* bufPtr = inBuffer + FREE_SPACE_OFFSET;
        int freeBytes = PAGE_SIZE - (sizeof(FreeSpaceLength) + sizeof(NumSlotsLength));
        memcpy(bufPtr, &freeBytes, sizeof(int));
        bufPtr-=sizeof(FreeSpaceLength);
        int numSlots = 0;
        memcpy(bufPtr, &numSlots, sizeof(int));
        fileHandle.appendPage(inBuffer);
        memcpy(page,inBuffer,PAGE_SIZE);
        free(inBuffer);
    }

    unsigned getPage(char *page, FileHandle &fileHandle, int recordSize) {
        if(fileHandle.getNumberOfPages() == 0){
            createNewPageDir(fileHandle, page);
            return fileHandle.getNumberOfPages()-1;
        }
        else{
            // read page
            char* data = (char*)malloc(PAGE_SIZE);
            fileHandle.readPage(fileHandle.getNumberOfPages()-1, data);
            int freeBytes = *(int*)(data + FREE_SPACE_OFFSET);
            free(data);

            if(recordSize + SLOT_SIZE > freeBytes){
                bool existingPageFound = false;
                int i=0;
                void* existingPageBuf= malloc(PAGE_SIZE);
                for(;i<fileHandle.getNumberOfPages();i++){
                    fileHandle.readPage(i,existingPageBuf);
                    char* existingPageBufPtr = (char*)existingPageBuf;
                    int existingPageFreeSpace = *(int*)(existingPageBufPtr+FREE_SPACE_OFFSET);
                    if(existingPageFreeSpace > recordSize + SLOT_SIZE){
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

    void copyRecordToPageBuf(const char* record, int recordLength, int seekLen, char* pagePtr){
        memcpy(pagePtr + seekLen, record, recordLength);
    }

    int findSlotToInsert(int &slotToInsert, char *lastSlot, int numSlots) {
        if (numSlots == 0) {
            slotToInsert = 0;
            return 1;
        }
        else {
            int i;
            for (i = 0; i < numSlots; i++) {
                short int offset = *(int*)lastSlot;
                if (offset == -1) {
                    break;
                }
                lastSlot = lastSlot - SLOT_SIZE;
            }
            slotToInsert = i;
            return (slotToInsert == numSlots) ? 1 : 0;
        }
    }

    void writeRecordToPage(const char* record, int recordSize, FileHandle &fileHandle, RID &rid) {
        char *page = new char[PAGE_SIZE];
        unsigned pageNum = getPage(page, fileHandle, recordSize);

        char *pagePtr = page;
        char *slotPtr = pagePtr + NUM_SLOTS_OFFSET;
        int numSlots = *(int *)(pagePtr + NUM_SLOTS_OFFSET);
        int currFree = *(int *)(page + FREE_SPACE_OFFSET);
        int seekLen = 0;
        int slotToInsert = 0;
        int isLast;

        if (numSlots == 0) {
            copyRecordToPageBuf(record, recordSize, seekLen, pagePtr);
            numSlots+=1;
        } else {
            isLast = findSlotToInsert(slotToInsert, slotPtr - SLOT_SIZE, numSlots);
            unsigned skipBytes = numSlots * sizeof(SlotLength);
            char *slot = slotPtr - skipBytes;
            short recordOffset = *(short *)slot;
            slot += sizeof(SlotSubFieldLength);
            short recordLength = *(short *)slot;
            seekLen = recordOffset + recordLength;

            if (!isLast) {
                int offset1 = seekLen;

                char *slotEnd = slotPtr - (numSlots * SLOT_SIZE);
                int offset2 = (*(short*) slotEnd) + (*(short*)(slotEnd + 2));

                memmove(page + offset1 + recordSize, page + offset1, offset2 - offset1);
                //update all the slots with offset + recordSize
                char *slotUpdate = slotPtr - skipBytes - (2 * SLOT_SIZE);
                for (int i = slotToInsert; i < numSlots - 1; i++) {
                    *(short int*)slotUpdate += recordSize;
                    slotUpdate -= SLOT_SIZE;
                }

            }

            copyRecordToPageBuf(record, recordSize, seekLen, pagePtr);
            if (isLast) numSlots+=1;
        }

        pagePtr = pagePtr + NUM_SLOTS_OFFSET;
        memcpy(pagePtr, &numSlots, sizeof(NumSlotsLength));
        pagePtr = pagePtr + NUM_SLOTS_BYTES;
        int newFree = currFree - recordSize - SLOT_SIZE; // reduce free size by slot (4 bytes) and record size
        memcpy(pagePtr,&newFree, sizeof(FreeSpaceLength));

        slotPtr = slotPtr - (slotToInsert + 1) * SLOT_SIZE;
        // add offset of new record
        memcpy(slotPtr,&seekLen, sizeof(short));
        slotPtr += sizeof(SlotSubFieldLength);
        // add length of new record
        memcpy(slotPtr, &recordSize, sizeof(short));

        fileHandle.writePage(pageNum, page);
        delete[] page;

        //RID
        rid.pageNum = (unsigned)pageNum;
        rid.slotNum = (unsigned short)slotToInsert;
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
        unsigned recordSize =  calculateFormattedRecordSize(nullAttributesIndicatorSize,recordDescriptor);
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
        short slotOffset = *(short*)(dataPointer + NUM_SLOTS_OFFSET - sizeof(SlotLength) * (slotNum + 1));
        short slotLength = *(short*)(dataPointer + NUM_SLOTS_OFFSET - sizeof(SlotLength) * (slotNum + 1) + sizeof(SlotSubFieldLength));
        return {slotOffset, slotLength};
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

    RID readRIDFromCharArray(const char* data) {
        RID rid;
        // Assuming the system uses little-endian and the data is stored accordingly
        // First 4 bytes are the page number
        memcpy(&rid.pageNum, data + sizeof(TombstoneByte), sizeof(rid.pageNum));

        // Next 2 bytes are the slot number
        memcpy(&rid.slotNum, data + sizeof(TombstoneByte) + sizeof(rid.pageNum), sizeof(rid.slotNum));

        return rid;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        unsigned pageNum = rid.pageNum;
        unsigned short slotNum = rid.slotNum;

        char* pageData = new char[PAGE_SIZE];
        memset(pageData, 0, PAGE_SIZE);

        if (fileHandle.readPage(pageNum, pageData)){
            std::cout << "Error reading page" << std::endl;
            delete[] pageData;
            return -1;
        }
        std::pair<short, short> offsetAndLength = readSlotInformation(pageData, slotNum);
        short recordOffset = offsetAndLength.first;
        short recordLength = offsetAndLength.second;

        if (recordOffset == -1){
            std::cout << "Cannot read deleted record" << std::endl;
            delete[] pageData;
            return -1;
        }

        char *record = new char[recordLength];
        // read recordLength bytes from recordOffset into memory buffer
        memcpy(record, pageData + recordOffset, recordLength);

        unsigned fieldCount = recordDescriptor.size();
        int nullAttributesIndicatorSize = getActualByteForNullsIndicator(fieldCount);

        // check if record is a tombstone
        if(decodeBytes(record[0])){
            // go to that page and load that record into memory
            // get RID from tombstone
            RID tsRid = readRIDFromCharArray(record);
            // char* actualPageData = new char[PAGE_SIZE]{};

            // go to OG record and get that record
            memset(pageData, 0, PAGE_SIZE);
            if (fileHandle.readPage(tsRid.pageNum, pageData)){
                std::cout << "Error reading tombstone page" << std::endl;
                delete[] pageData;
                return -1;
            }

            std::pair<short, short> actualOffsetAndLength = readSlotInformation(pageData, tsRid.slotNum);
            recordOffset = actualOffsetAndLength.first;
            recordLength = actualOffsetAndLength.second;
            delete[] record;
            record = new char[recordLength];
            memcpy(record, pageData + recordOffset, recordLength);

        }

        char *recordPointer = record;
        recordPointer += sizeof(TombstoneByte ) + sizeof(NumFieldsLength);

        //find out which fields are NULL and set bits for NULL fields
        std::vector<bool> isNull;
        processBitVector(isNull, nullAttributesIndicatorSize, recordPointer);

        int deserializedRecordSize =  getDeserializedRecordSize(recordDescriptor, nullAttributesIndicatorSize);
        char *deserializedRecordBuf = new char[deserializedRecordSize];
        memset(deserializedRecordBuf, 0, deserializedRecordSize);

        char *dataPointer = deserializedRecordBuf;
        memcpy(dataPointer, recordPointer, nullAttributesIndicatorSize);

        recordPointer += nullAttributesIndicatorSize;
        dataPointer += nullAttributesIndicatorSize;

        char *offsetPointer = recordPointer;
        recordPointer += fieldCount * sizeof (OffsetLength);

        for (int i = 0; i < fieldCount; i++) {
            if (isNull[i]) {
                offsetPointer += sizeof(OffsetLength);
                continue;
            }
            switch (recordDescriptor[i].type) {
                case PeterDB::TypeInt: // int
                case PeterDB::TypeReal: { // float
                    // Since the size of int and float is the same, we can handle them in the same case
                    memcpy(dataPointer, recordPointer, sizeof(int));
                    recordPointer += sizeof(int);
                    dataPointer += sizeof(int);
                    offsetPointer += sizeof(OffsetLength);
                    break;
                }
                case PeterDB::TypeVarChar: { // string
                    int stringLen = record + *(int *) offsetPointer - recordPointer;
                    memcpy((int *) dataPointer, &stringLen, sizeof(VarCharLength));
                    dataPointer += sizeof(VarCharLength);

                    memcpy(dataPointer, recordPointer, stringLen);

                    dataPointer += stringLen;
                    recordPointer += stringLen;
                    offsetPointer += sizeof(OffsetLength);
                    break;
                }
            }
        }

        memcpy(data, deserializedRecordBuf, dataPointer - deserializedRecordBuf);
        delete[] pageData;
        delete[] record;
        delete[] deserializedRecordBuf;
        return 0;
    }

    void getOrSetFreeSpace (char *page, int &freeBytes, int operation) {
        int *freeSpacePointer = (int*)(page + PAGE_SIZE - 1 - sizeof (int));
        if (operation == 0) {
            freeBytes = *freeSpacePointer;
        }
        else {
            *freeSpacePointer = freeBytes;
        }
    }
    void getOrSetNumSlots (char *page, int &numRecords, int operation) {
        int *numRecordsPointer = (int*)(page + PAGE_SIZE - 1 - (2 * sizeof (int)));
        if (operation == 0) {
            numRecords = *numRecordsPointer;
        }
        else {
            *numRecordsPointer = numRecords;
        }
    }

    void getRecordInfo (char *page, int &recordOffset, int &length, int slotNumber) {
        char *slotPointer = (page + PAGE_SIZE - 1 - (2 * sizeof (int)) - (slotNumber + 1) * sizeof (int));

        recordOffset = *(short int*) slotPointer;
        length       = *(short int*) (slotPointer + sizeof (short int));
    }

    void deleteRecordAndShiftBytes(FileHandle& fileHandle, const RID& rid, char* page){

        int freeBytes, numSlots;
        int recordOffset, recordLength;
        getOrSetFreeSpace(page, freeBytes, 0);
        getOrSetNumSlots(page, numSlots, 0);
        getRecordInfo(page, recordOffset, recordLength, rid.slotNum);

        // shift left
        int startingOffset = recordOffset + recordLength;
        int endingOffset = NUM_SLOTS_OFFSET - (numSlots * SLOT_SIZE) - freeBytes;
        if((endingOffset - startingOffset) > 0){
            memmove(page + recordOffset, page + startingOffset, endingOffset - startingOffset);
        }

        char *slotPointer = (page + NUM_SLOTS_OFFSET - (rid.slotNum + 1) * SLOT_SIZE);
        // set offset of deleted record to -1
        *(short int*)slotPointer = -1;

        freeBytes += recordLength;
        getOrSetFreeSpace(page, freeBytes, 1);
        //increase the free space in the page
        //update all other offsets
//        slotPointer -= SLOT_SIZE;
        for (int i = rid.slotNum; i < numSlots; i++) {
            if((*(short*)slotPointer) != -1){
                *(short*)slotPointer -= recordLength;
            }

            slotPointer -= SLOT_SIZE;
        }

        fileHandle.writePage(rid.pageNum, page);
        delete[] page;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {

        char *pageData = new char[PAGE_SIZE];
        if (fileHandle.readPage(rid.pageNum, pageData)){
            std::cout << "Error reading pageData" << std::endl;
            return -1;
        }

        std::pair<short, short> offsetAndLength = readSlotInformation(pageData, rid.slotNum);
        short recordOffset = offsetAndLength.first;
        short recordLength = offsetAndLength.second;
        char* record = new char[recordLength];
        memcpy(record, pageData + recordOffset, recordLength);
//
//        // check if record is tombstone
        bool isRecTombstone =  decodeBytes(record[0]);

        // if yes, delete tombstone and og
        if(isRecTombstone){
            //delete record
            RID tsRid;
            memcpy(&tsRid.pageNum, record + sizeof(TombstoneByte), sizeof(tsRid.pageNum));
            memcpy(&tsRid.slotNum, record + sizeof(TombstoneByte) + sizeof(tsRid.pageNum), sizeof(tsRid.slotNum));
            char* tombStonePageData = new char[PAGE_SIZE];
            if (fileHandle.readPage(tsRid.pageNum, tombStonePageData)){
                std::cout << "Error reading page data" << std::endl;
                return -1;
            }
            deleteRecordAndShiftBytes(fileHandle, tsRid, tombStonePageData);
            //delete tombstone
            deleteRecordAndShiftBytes(fileHandle, rid, pageData);

        }
        else{
            deleteRecordAndShiftBytes(fileHandle, rid, pageData);
        }
        delete[] record;
        return 0;
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
        char* pageData = new char[PAGE_SIZE]{};
        char* tombstoneRecord = nullptr;
        char* tombStonePageData = nullptr;
        short tombStoneOffset, tombstoneLength;

        RID tsRid;

        if (fileHandle.readPage(rid.pageNum, pageData)){
            std::cout << "Error reading page" << std::endl;
            return -1;
        }

        std::pair<short, short> offsetAndLength = readSlotInformation(pageData, rid.slotNum);
        short oldRecordOffset = offsetAndLength.first;
        short oldRecordLength = offsetAndLength.second;
        char* oldRecord = new char[oldRecordLength];
        memcpy(oldRecord, pageData + oldRecordOffset, oldRecordLength);

        bool isOldRecTombstone =  decodeBytes(oldRecord[0]);
        if(isOldRecTombstone){
            memcpy(&tsRid.pageNum, oldRecord + sizeof(TombstoneByte), sizeof(tsRid.pageNum));
            memcpy(&tsRid.slotNum, oldRecord + sizeof(TombstoneByte) + sizeof(tsRid.pageNum), sizeof(tsRid.slotNum));
            tombStonePageData = new char[PAGE_SIZE];
            memcpy(tombStonePageData, pageData, PAGE_SIZE);
            tombstoneRecord = new char[oldRecordLength];
            memcpy(tombstoneRecord, oldRecord, oldRecordLength);
            tombStoneOffset = oldRecordOffset;
            tombstoneLength = oldRecordLength;
            delete[] oldRecord;
            fileHandle.readPage(tsRid.pageNum, pageData);
            offsetAndLength = readSlotInformation(pageData, tsRid.slotNum);
            oldRecordOffset = offsetAndLength.first;
            oldRecordLength = offsetAndLength.second;
            oldRecord = new char[oldRecordLength];
            memcpy(oldRecord, pageData + oldRecordOffset, oldRecordLength);

        }

        int freeBytes = *(int*)(pageData+ FREE_SPACE_OFFSET);
        int numSlots = *(int*)(pageData + NUM_SLOTS_OFFSET);
        // calculate size of new record
        int nullAttributesIndicatorSize = getActualByteForNullsIndicator(recordDescriptor.size());
        std::vector<bool> isNull;

        // Set bits for NULL fields
        processBitVector(isNull, nullAttributesIndicatorSize, data);

        // Calculate formatted record size
        unsigned updatedRecordLength =  calculateFormattedRecordSize(nullAttributesIndicatorSize,recordDescriptor);
        char *updatedRecord = nullptr;

        buildRecord(&updatedRecordLength, updatedRecord, recordDescriptor, data, nullAttributesIndicatorSize, isNull);

        if (updatedRecordLength > PAGE_SIZE) {
            return -1;
        }

        if(updatedRecordLength < oldRecordLength){
            //left shift
            int startingOffset = oldRecordOffset + oldRecordLength;
            int endingOffset = NUM_SLOTS_OFFSET - (numSlots * SLOT_SIZE) - freeBytes;
            int sizeToShift = endingOffset - startingOffset;
            memmove(pageData + oldRecordOffset + updatedRecordLength, pageData + startingOffset, sizeToShift);
            //copy updated record
            memcpy(pageData+oldRecordOffset, updatedRecord, updatedRecordLength);
            //update slots
            unsigned short shiftBytesCount = oldRecordLength - updatedRecordLength;
            char* slotPtr = pageData+NUM_SLOTS_OFFSET- (numSlots * SLOT_SIZE);
            for(int i=0;i<(numSlots - (rid.slotNum+1));i++){
                short currOffset = *(short*)slotPtr;
                short newOffset = currOffset - shiftBytesCount;
                memcpy(slotPtr, &newOffset, sizeof(SlotSubFieldLength));
                slotPtr+=SLOT_SIZE;
            }
            // update length of updated record
            memcpy(slotPtr + sizeof(SlotSubFieldLength), &updatedRecordLength, sizeof(SlotSubFieldLength));
            if(isOldRecTombstone){
                fileHandle.writePage(tsRid.pageNum, pageData);
            }
            else{
                fileHandle.writePage(rid.pageNum, pageData);
            }
        }
        else if(updatedRecordLength == oldRecordLength){
            memcpy(pageData+oldRecordOffset, updatedRecord, updatedRecordLength);
            if(isOldRecTombstone){
                fileHandle.writePage(tsRid.pageNum, pageData);
            }
            else{
                fileHandle.writePage(rid.pageNum, pageData);
            }
        }
        else if(updatedRecordLength > oldRecordLength){
            if(updatedRecordLength <= freeBytes){
                // right shift
                int startingOffset = oldRecordOffset + oldRecordLength;
                int endingOffset = NUM_SLOTS_OFFSET - (numSlots * SLOT_SIZE) - freeBytes;
                int sizeToShift = endingOffset - startingOffset;
                memmove(pageData + oldRecordOffset + updatedRecordLength, pageData + startingOffset, sizeToShift);
                //copy updated record
                memcpy(pageData+oldRecordOffset, updatedRecord, updatedRecordLength);
                //update slots after updated record slot
                unsigned short shiftBytesCount = updatedRecordLength - oldRecordLength;
                char* slotPtr = pageData+NUM_SLOTS_OFFSET - (numSlots * SLOT_SIZE);
                for(int i=0;i<(numSlots - (rid.slotNum+1));i++){
                    short currOffset = *(short*)slotPtr;
                    short newOffset = currOffset + shiftBytesCount;
                    memcpy(slotPtr, &newOffset, sizeof(SlotSubFieldLength));
                    slotPtr+=SLOT_SIZE;
                }
                // update length of updated record
                memcpy(slotPtr + sizeof(SlotSubFieldLength), &updatedRecordLength, sizeof(SlotSubFieldLength));
                if(isOldRecTombstone){
                    fileHandle.writePage(tsRid.pageNum, pageData);
                }
                else{
                    fileHandle.writePage(rid.pageNum, pageData);
                }
            }
            else{
                // find place for updated record
                RID newRid;
                writeRecordToPage(updatedRecord, updatedRecordLength, fileHandle, newRid);

                if(isOldRecTombstone){
                    //update tombstone RID
                    memcpy(tombstoneRecord + sizeof(TombstoneByte), &newRid.pageNum, sizeof(unsigned));
                    memcpy(tombstoneRecord + sizeof(TombstoneByte) + sizeof(unsigned),&newRid.slotNum,
                           sizeof(unsigned short));
                    // delete old record
                    deleteRecord(fileHandle, recordDescriptor, tsRid);
                    memcpy(tombStonePageData+tombStoneOffset, tombstoneRecord, TOMBSTONE_SIZE);
                    fileHandle.writePage(rid.pageNum, tombStonePageData);
                }
                else{
                    // create a tombstone
                    char* tombStone = new char[TOMBSTONE_SIZE];
                    char tombStoneByte;
                    encodeBytes(true, tombStoneByte);
                    memcpy(tombStone, &tombStoneByte, sizeof(TombstoneByte));
                    memcpy(tombStone + sizeof(TombstoneByte), &newRid.pageNum, sizeof(unsigned));
                    memcpy(tombStone + sizeof(TombstoneByte) + sizeof(unsigned), &newRid.slotNum,
                           sizeof(unsigned short));
                    // shift left
                    int startingOffset = oldRecordOffset + oldRecordLength;
                    int endingOffset = NUM_SLOTS_OFFSET - (numSlots * SLOT_SIZE) - freeBytes;
                    int sizeToShift = endingOffset - startingOffset;
                    memmove(pageData + oldRecordOffset + TOMBSTONE_SIZE, pageData + startingOffset, sizeToShift);
                    //copy tombstone
                    memcpy(pageData+oldRecordOffset, tombStone, TOMBSTONE_SIZE);
                    //update slots of successive record
                    unsigned short shiftBytesCount = oldRecordLength - TOMBSTONE_SIZE;
                    char* slotPtr = pageData + NUM_SLOTS_OFFSET - (numSlots * SLOT_SIZE);
                    for(int i=0;i<(numSlots - (rid.slotNum+1));i++){
                        short currOffset = *(short*)slotPtr;
                        short newOffset = currOffset - shiftBytesCount;
                        memcpy(slotPtr, &newOffset, sizeof(SlotSubFieldLength));
                        slotPtr+=SLOT_SIZE;
                    }
                    // update length of updated record
                    int tsSize = TOMBSTONE_SIZE;
                    memcpy(slotPtr + sizeof(SlotSubFieldLength), &tsSize, sizeof(SlotSubFieldLength));
                    fileHandle.writePage(rid.pageNum, pageData);
                    delete[] tombStone;


                }

            }
        }
        delete[] tombStonePageData;
        delete[] tombstoneRecord;
        delete[] updatedRecord;
        delete[] oldRecord;
        delete[] pageData;
        return 0;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        rbfm_ScanIterator.fileHandle = fileHandle;
        rbfm_ScanIterator.attributeNames = attributeNames;
        rbfm_ScanIterator.recordDescriptor = recordDescriptor;
        rbfm_ScanIterator.compOp = compOp;
        rbfm_ScanIterator.conditionAttribute = conditionAttribute;
        rbfm_ScanIterator.currentPage = 0;
        rbfm_ScanIterator.slotNum = 0;

        for (int i = 0; i < recordDescriptor.size(); i++) {
            rbfm_ScanIterator.attributePositions[recordDescriptor[i].name] = i;
        }
        if (compOp != NO_OP) {
            if (rbfm_ScanIterator.attributePositions.find(conditionAttribute) != rbfm_ScanIterator.attributePositions.end())
                rbfm_ScanIterator.compValType = recordDescriptor[rbfm_ScanIterator.attributePositions[conditionAttribute]].type;

            else {
                std::cout << "Attribute doesn't exist " << std::endl;
                return -1;
            }

            if (rbfm_ScanIterator.compValType == TypeVarChar) {
                int length = *(int*)value;
                rbfm_ScanIterator.value = (char*)malloc(length + sizeof (int));
                memcpy(rbfm_ScanIterator.value, value, length + sizeof (int));
            }
            else{
                rbfm_ScanIterator.value = (char*)malloc(sizeof (int));
                memcpy(rbfm_ScanIterator.value, value, sizeof (int));
            }
        }
        return 0;
    }

    RC readStoredRecord (RID &rid, char *&record, char *page) {
        char *slotPointer = page + PAGE_SIZE - 1 - (NUM_SLOTS_BYTES + FREE_SPACE_BYTES);
        char *slotRequired = slotPointer - (rid.slotNum + 1) * SLOT_SIZE;

        int offset = *(short*)slotRequired;
        int length = *((short*)(slotRequired + sizeof (SlotSubFieldLength)));

        record = (char*) malloc(length);
        memset(record, 0, length);
        memcpy(record, page + offset ,length);
        return 0;
    }
    RC checkAttributeNull (char *record, int position) {
        int bitsetPosition = position / CHAR_BIT;
        std::bitset<8> Bitset;
        memcpy(&Bitset, record + sizeof (int) + sizeof (TombstoneByte) + bitsetPosition, 1);
//        std::cout << Bitset << std::endl;
        return (Bitset.test(position % CHAR_BIT));
    }
    RC readSingleAttribute (char *record, char *&attributeValue, int position, AttrType type, int numFields) {
        int bitVectorSize = getActualByteForNullsIndicator (numFields);
        if (checkAttributeNull(record, position))
            return -1;

        char *offsetPointer = record + sizeof (int) + sizeof (TombstoneByte) + bitVectorSize;
        char *slotRequired = offsetPointer + (position * sizeof (OffsetLength));
        char *starting, *ending;

        ending = record + (*(int*)slotRequired);

        if (position == 0) {
            starting = offsetPointer + (numFields * sizeof (OffsetLength));
        } else {
            starting = record + (*(int*)(slotRequired - sizeof (OffsetLength)));
        }

        int totalSize = ending - starting;
        if (type == TypeVarChar) {
            attributeValue = (char*) malloc(totalSize + sizeof (int));
            *(int*)attributeValue = totalSize;
            memcpy(attributeValue + sizeof (totalSize), starting, ending - starting);
        }
        else {
            attributeValue = (char*) malloc(totalSize);
            memcpy(attributeValue, starting, ending - starting);
        }

        return 0;
    }
    RC compareIntegerAttributes (int a, int b, CompOp compOp) {
        switch (compOp) {
            case EQ_OP: return (a == b);
            case LT_OP: return (a < b);
            case LE_OP: return (a <= b);
            case GT_OP: return (a > b);
            case GE_OP: return (a >= b);
            case NE_OP: return (a != b);
        }
    }

    RC compareRealAttributes (float a, float b, CompOp compOp) {
        switch (compOp) {
            case EQ_OP: return (a == b);
            case LT_OP: return (a < b);
            case LE_OP: return (a <= b);
            case GT_OP: return (a > b);
            case GE_OP: return (a >= b);
            case NE_OP: return (a != b);
        }
    }
    RC compareVarCharAttributes(std::string a, std::string b, CompOp compOp) {
        switch (compOp) {
            case EQ_OP: return (a == b);
            case LT_OP: return (a < b);
            case LE_OP: return (a <= b);
            case GT_OP: return (a > b);
            case GE_OP: return (a >= b);
            case NE_OP: return (a != b);
        }
    }

    RC compareAttributes (void *attribute, void *value, CompOp compOp, AttrType type) {
        if (type == TypeVarChar){
            std::string attrString((char*)((char*)attribute + sizeof (int)), *(int*)attribute);
            std::string valString((char*)value + sizeof (int), *(int*)value);

            return (false == compareVarCharAttributes(attrString, valString, compOp));

        } else if (type == TypeInt) {
            int attrInt = *(int*)attribute;
            int valInt = *(int*)value;
            return (false == compareIntegerAttributes (attrInt, valInt, compOp));
        }
        else {
            float attrReal = *(float*)attribute;
            float valReal = *(float*)value;
            return (false == compareRealAttributes(attrReal, valReal, compOp));
        }
        return 0;
    }
    void processSelectedAttributes(const std::vector<std::string>&attributeNames, std::unordered_map<std::string, int> &attributePositions,
                                   char *&result, std::vector<bool> isNull){
        size_t selectedFieldSize = attributeNames.size();
        result = (char*) malloc((selectedFieldSize+7) /8);
        memset(result, 0, (selectedFieldSize + 7) / 8);
        std::vector<int> positions (selectedFieldSize);
        for(int i=0; i < selectedFieldSize; i++){
            positions[i] = attributePositions[attributeNames[i]];
            //std::cout << attributeNames[i] << positions[i] << std::endl;
        }
        std::sort(positions.begin(), positions.begin() + selectedFieldSize);
        for (int i = 0; i < selectedFieldSize; ++i) {
            // Calculate destination position
            int destByteIndex = i / 8;
            int destBitIndex = i % 8;

            if (isNull[positions[i]]) {
                result[destByteIndex] |= (1 << (7 - destBitIndex));
            }
        }
        std::bitset<8> Bitset;
        memcpy(&Bitset, result, 1);
        //std::cout << isNull[1] << " "<<Bitset << std::endl;
    }
    RC buildSelectedAttributesRecord (char *record, const std::vector<Attribute>&recordDescriptor,
                                      const std::vector<std::string>&attributeNames,
                                      void *&data, std::unordered_map<std::string, int> &attributePositions) {

        char *OGRecordPointer = record + sizeof (int) + sizeof (TombstoneByte);
        int fieldCount = recordDescriptor.size();
        std::vector <bool> validAttributesIndex (recordDescriptor.size(), false);
        for (auto i: attributeNames) {
            validAttributesIndex[attributePositions[i]] = true;
        }

        int originalBitVectorSize = getActualByteForNullsIndicator(fieldCount);

        std::vector<bool> isNull;
        char *bitVectorPointer = record + sizeof (int) + sizeof (TombstoneByte);

        for (int i = 0; i < originalBitVectorSize; i++) {
            for (int bit = CHAR_BIT - 1; bit >= 0; --bit) {
                isNull.push_back((((char*)bitVectorPointer)[i] & (1 << bit)) != 0);
            }
        }

        int newBitVectorSize = getActualByteForNullsIndicator(attributeNames.size());
        int total_size = newBitVectorSize;
        for (auto attributeName : attributeNames) {
            int i = attributePositions[attributeName];
            total_size += recordDescriptor[i].length;
            if (recordDescriptor[i].type == TypeVarChar)
                total_size += sizeof (int);
        }

        char deserializedRecord [total_size];
        char *deSerRecordPointer = deserializedRecord;
        memset(deserializedRecord, 0, total_size);

        char *bitvector;
        processSelectedAttributes (attributeNames, attributePositions, bitvector, isNull);
        memcpy(deSerRecordPointer, bitvector, newBitVectorSize);

        deSerRecordPointer += newBitVectorSize;
        OGRecordPointer += originalBitVectorSize;
        char *OGRecordDataPointer = OGRecordPointer + (fieldCount * sizeof (OffsetLength));
        char *OGRecordOffsetPointer = OGRecordPointer;

        for (int i = 0; i < fieldCount; i++) {
            if (isNull[i] || !validAttributesIndex[i]) {
                //std::cout << i << std::endl;
                OGRecordDataPointer = record + *(int*)OGRecordOffsetPointer;
                OGRecordOffsetPointer += sizeof (OffsetLength);
                if (isNull[i])
                    total_size -= recordDescriptor[i].length;

                continue;
            }
            if (recordDescriptor[i].type == 0) {
                int int_data;
                memcpy(&int_data, OGRecordDataPointer, sizeof (int));
                OGRecordDataPointer += sizeof (int);

                memcpy((int*)deSerRecordPointer, &int_data, sizeof (int));
                deSerRecordPointer += sizeof (int);

                OGRecordOffsetPointer += sizeof (OffsetLength) ;
            }
            else if (recordDescriptor[i].type == 1) {
                float float_data;
                memcpy(&float_data, OGRecordDataPointer, sizeof (float));
                OGRecordDataPointer += sizeof (float);

                memcpy((int*)deSerRecordPointer, &float_data, sizeof (float));
                deSerRecordPointer += sizeof (float);

                OGRecordOffsetPointer += sizeof (OffsetLength) ;
            }
            else if (recordDescriptor[i].type == 2) {
                total_size -= recordDescriptor[i].length;
                int length_of_string = record + *(int*)OGRecordOffsetPointer - OGRecordDataPointer;
                memcpy((int*)deSerRecordPointer, &length_of_string, sizeof (int));
                deSerRecordPointer += 4;

                total_size += length_of_string;
                memcpy(deSerRecordPointer, OGRecordDataPointer, length_of_string);
                deSerRecordPointer += length_of_string;
                OGRecordDataPointer += length_of_string;
                OGRecordOffsetPointer += sizeof (OffsetLength);
            }
        }
        memcpy(data, deserializedRecord , total_size);
        return 0;
    }
    RC checkTombstone (char *record) {
        char tombstoneByte;
        memcpy(&tombstoneByte, record, 1);
        return tombstoneByte;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *&data) {
        //TODO: test for empty strings
        int reqSatisfied = 0;
        while (!reqSatisfied){
            if (currentPage == fileHandle.getNumberOfPages())
                return RBFM_EOF;

            char page [PAGE_SIZE];
            fileHandle.readPage(currentPage, page);
            int freeSpace = 0;
            int numSlots = 0;
            getOrSetFreeSpace(page, freeSpace, 0);
            getOrSetNumSlots(page, numSlots, 0);

            if (slotNum == numSlots) {
                currentPage ++;
                continue;
            }

            RID ridCheck;
            ridCheck.pageNum = currentPage;
            ridCheck.slotNum = slotNum;

            char *record = nullptr;
            readStoredRecord (ridCheck, record, page);
            if (checkTombstone(record)) {
                free(record);
                slotNum++;
                continue;
            }

            //check attribute first if there is some condition
            if (compOp != NO_OP) {
                char *attributeValue;
                int numFields = *(int*)(record + sizeof (TombstoneByte));
                if (readSingleAttribute(record, attributeValue,
                                        attributePositions[conditionAttribute],
                                        compValType,
                                        numFields)) {
                    free(record);
                    free(attributeValue);
                    slotNum ++;
                    continue;
                }

                if (0 != compareAttributes(attributeValue, value, compOp,compValType)) {
                    //checking if the selected attribute is NULL or not satisfying the condition
                    free(record);
                    free(attributeValue);
                    slotNum ++;
                    continue;
                }
            }

            //after confirming that condition is satisfied, build the record to be returned
            buildSelectedAttributesRecord (record, recordDescriptor, attributeNames, data, attributePositions);
            rid.pageNum = ridCheck.pageNum;
            rid.slotNum = ridCheck.slotNum;

            reqSatisfied = 1;
            slotNum ++;
        }
        return 0;
    }

} // namespace PeterDB

