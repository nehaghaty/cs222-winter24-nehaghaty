#include "src/include/rm.h"
#include "src/include/ix.h"
#include <iostream>
#include <cstring>
#include <sys/stat.h>
#include <cmath>
#include <algorithm>

#define CHAR_BIT    8

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    FileHandle tablesFileHandle;
    FileHandle attrsFileHandle;
    FileHandle indicesFileHandle;
    std::string tablesFileName = "Tables";
    std::string attrsFileName = "Columns";
    std::string indicesFileName = "IX_Table";
    std::vector<PeterDB::Attribute> tablesRecordDescriptor;
    std::vector<PeterDB::Attribute> attrsRecordDescriptor;
    std::vector<PeterDB::Attribute> indicesRecordDescriptor;

    bool canAccess(std::string tableName){
        if(tableName == tablesFileName || tableName == attrsFileName){
            return false;
        }
        return true;
    }

    static void findPositionInRD (std::vector<Attribute> &attrs, int &position, std::string &attributeName) {
        position = 0;
        int i;
        for (i = 0; i < attrs.size(); i++) {
            if (attrs[i].name == attributeName)
                break;
        }
        position = i;
    }

    void addAttribute(std::vector<PeterDB::Attribute> &recordDescriptor,
                      const std::string &name,
                      PeterDB::AttrType type,
                      PeterDB::AttrLength length) {
        PeterDB::Attribute attr;
        attr.name = name;
        attr.type = type;
        attr.length = length;
        recordDescriptor.push_back(attr);
    }

    void createTableRecordDescriptor(std::vector<PeterDB::Attribute> &recordDescriptor) {
        addAttribute(recordDescriptor, "table-id", PeterDB::TypeInt, 4);
        addAttribute(recordDescriptor, "table-name", PeterDB::TypeVarChar, 50);
        addAttribute(recordDescriptor, "file-name", PeterDB::TypeVarChar, 50);
    }

    void createAttrRecordDescriptor(std::vector<PeterDB::Attribute> &recordDescriptor) {
        addAttribute(recordDescriptor, "table-id", PeterDB::TypeInt, 4);
        addAttribute(recordDescriptor, "column-name", PeterDB::TypeVarChar, 50);
        addAttribute(recordDescriptor, "column-type", PeterDB::TypeInt, 4);
        addAttribute(recordDescriptor, "column-length", PeterDB::TypeInt, 4);
        addAttribute(recordDescriptor, "column-position", PeterDB::TypeInt, 4);
    }

    void createIndicesRecordDescriptor(std::vector<PeterDB::Attribute> &recordDescriptor) {
        addAttribute(recordDescriptor, "table-id", PeterDB::TypeInt, 4);
        addAttribute(recordDescriptor, "attr-name", PeterDB::TypeVarChar, 50);
        addAttribute(recordDescriptor, "attr-type", PeterDB::TypeInt, 4);
        addAttribute(recordDescriptor, "index-filename", PeterDB::TypeVarChar, 50);
    }

    static unsigned char *initializeNullFieldsIndicator(const std::vector<PeterDB::Attribute> &recordDescriptor) {
        int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
        auto indicator = new unsigned char[nullFieldsIndicatorActualSize];
        memset(indicator, 0, nullFieldsIndicatorActualSize);
        return indicator;
    }

    static void prepareTablesRecord(size_t fieldCount,
                                    unsigned char *nullFieldsIndicator,
                                    const int id,
                                    const int nameLength,
                                    const std::string &name,
                                    const int fileNameLength,
                                    const std::string &fileName,
                                    void *buffer) {
        int offset = 0;

        // Null-indicators
        int nullFieldsIndicatorActualSize = ceil((double) fieldCount / CHAR_BIT);

        // Null-indicator for the fields
        memcpy((char *) buffer + offset, nullFieldsIndicator,
               nullFieldsIndicatorActualSize);
        offset += nullFieldsIndicatorActualSize;

        // id field
        memcpy((char *) buffer + offset, &id, sizeof(int));
        offset += sizeof(int);

        // name field
        memcpy((char *) buffer + offset, &nameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, name.c_str(), nameLength);
        offset += nameLength;

        // filename field
        memcpy((char *) buffer + offset, &fileNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, fileName.c_str(), fileNameLength);
    }

    static void prepareAttrsRecord(size_t fieldCount,
                                   unsigned char *nullFieldsIndicator,
                                   const int tableId,
                                   const int attrNameLength,
                                   const std::string &attrName,
                                   const int attrType,
                                   const int attrLen,
                                   const int position,
                                   void *buffer) {
        int offset = 0;
        // Null-indicators
        int nullFieldsIndicatorActualSize = ceil((double) fieldCount / CHAR_BIT);

        // Null-indicator for the fields
        memcpy((char *) buffer + offset, nullFieldsIndicator,
               nullFieldsIndicatorActualSize);
        offset += nullFieldsIndicatorActualSize;

        // table id field
        memcpy((char *) buffer + offset, &tableId, sizeof(int));
        offset += sizeof(int);

        // attribute name field
        memcpy((char *) buffer + offset, &attrNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, attrName.c_str(), attrNameLength);
        offset += attrNameLength;

        // attribute type field
        memcpy((char *) buffer + offset, &attrType, sizeof(int));
        //std::cout << "After adding type " << *(int*)((char*)buffer + offset) << std::endl;
        offset += sizeof(int);

        // attribute length field
        memcpy((char *) buffer + offset, &attrLen, sizeof(int));
        //std::cout << "After adding type " << *(int*)((char*)buffer + offset) << std::endl;
        offset += sizeof(int);

        // position field
        memcpy((char *) buffer + offset, &position, sizeof(int));
        offset += sizeof(int);
    }

    static void prepareIxRecord(size_t fieldCount,
                                   unsigned char *nullFieldsIndicator,
                                   const int tableId,
                                   const int attrNameLength,
                                   const std::string &attrName,
                                   const int attrType,
                                   const int indexFilenameLength,
                                   const std::string &indexFileName,
                                   void *buffer) {
        int offset = 0;
        // Null-indicators
        int nullFieldsIndicatorActualSize = ceil((double) fieldCount / CHAR_BIT);

        // Null-indicator for the fields
        memcpy((char *) buffer + offset, nullFieldsIndicator,
               nullFieldsIndicatorActualSize);
        offset += nullFieldsIndicatorActualSize;

        // table id field
        memcpy((char *) buffer + offset, &tableId, sizeof(int));
        offset += sizeof(int);

        // attribute name field
        memcpy((char *) buffer + offset, &attrNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, attrName.c_str(), attrNameLength);
        offset += attrNameLength;

        // attribute type field
        memcpy((char *) buffer + offset, &attrType, sizeof(int));
        offset += sizeof(int);

        // index filename field
        memcpy((char *) buffer + offset, &indexFilenameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, indexFileName.c_str(), indexFilenameLength);

    }

    RC RelationManager::createCatalog() {

        // create files and record descriptors for Tables table, Attributes table and IX_Table
        RecordBasedFileManager::instance().createFile(tablesFileName);
        RecordBasedFileManager::instance().openFile(tablesFileName, tablesFileHandle);
        createTableRecordDescriptor(tablesRecordDescriptor);

        RecordBasedFileManager::instance().createFile(attrsFileName);
        RecordBasedFileManager::instance().openFile(attrsFileName, attrsFileHandle);
        createAttrRecordDescriptor(attrsRecordDescriptor);

        RecordBasedFileManager::instance().createFile(indicesFileName);
        RecordBasedFileManager::instance().openFile(indicesFileName, indicesFileHandle);
        createIndicesRecordDescriptor(indicesRecordDescriptor);

        /********************
        * Tables table *
        *********************/
        // prepare records
        unsigned char *nullsIndicator = initializeNullFieldsIndicator(tablesRecordDescriptor);
        void* tablesBuffer = malloc(100);
        void* attrsBuffer = malloc(100);
        void* indicesBuffer = malloc(100);
        prepareTablesRecord(tablesRecordDescriptor.size(), nullsIndicator, 0, tablesFileName.length(),
                            tablesFileName, tablesFileName.length(), tablesFileName, tablesBuffer);
        prepareTablesRecord(tablesRecordDescriptor.size(), nullsIndicator, 1, attrsFileName.length(),
                            attrsFileName, attrsFileName.length(), attrsFileName, attrsBuffer);
        prepareTablesRecord(tablesFileName.size(), nullsIndicator, 2, indicesFileName.length(),
                            indicesFileName, indicesFileName.length(), indicesFileName, indicesBuffer);

        // insert records
        RID tablesRid;
        RecordBasedFileManager::instance().insertRecord(tablesFileHandle, tablesRecordDescriptor, tablesBuffer, tablesRid);
        RID attrsRid;
        RecordBasedFileManager::instance().insertRecord(tablesFileHandle, tablesRecordDescriptor, attrsBuffer, attrsRid);
        RID ixRid;
        RecordBasedFileManager::instance().insertRecord(tablesFileHandle, tablesRecordDescriptor, indicesBuffer, ixRid);
        free(tablesBuffer);
        free(attrsBuffer);
        free(indicesBuffer);

        /********************
        * Attributes table *
        *********************/
        // prepare Tables records
        void* idRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 0, 8, "table-id", PeterDB::TypeInt,4, 1, idRecBuf);
        void* nameRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 0, 10, "table-name", PeterDB::TypeVarChar,50, 2, nameRecBuf);
        void* filenameRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 0, 9, "file-name", PeterDB::TypeVarChar, 50, 3, filenameRecBuf);

        // insert Tables records into Attributes table
        RID idRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, idRecBuf, idRid);
        RID nameRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, nameRecBuf, nameRid);
        RID filenameRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, filenameRecBuf, filenameRid);

        free(idRecBuf);
        free(nameRecBuf);
        free(filenameRecBuf);

        // prepare Attributes records
        void* tableIdRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 1, 8,"table-id", PeterDB::TypeInt, 4, 1, tableIdRecBuf);
        void* attrNameRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 1, 11, "column-name", PeterDB::TypeVarChar, 50, 2, attrNameRecBuf);
        void* attrTypeRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 1, 11, "column-type", PeterDB::TypeInt, 4, 3, attrTypeRecBuf);
        void* attrLenRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 1, 13, "column-length", PeterDB::TypeInt, 4, 4, attrLenRecBuf);
        void* positionRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 1, 15, "column-position", PeterDB::TypeInt,4, 5, positionRecBuf);

        // insert Attributes records into Attributes table
        RID tableIdRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, tableIdRecBuf, tableIdRid);
        RID attrNameRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, attrNameRecBuf, attrNameRid);
        RID attrTypeRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, attrTypeRecBuf, attrTypeRid);
        RID attrLenRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, attrLenRecBuf, attrLenRid);
        RID posRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, positionRecBuf, posRid);

        free(tableIdRecBuf);
        free(attrNameRecBuf);
        free(attrTypeRecBuf);
        free(attrLenRecBuf);
        free(positionRecBuf);

        // prepare IX_Table records
        void* tidRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 2, 8,"table-id", PeterDB::TypeInt, 4, 1, tidRecBuf);
        void* ixAttrNameRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 2, 9, "attr-name", PeterDB::TypeVarChar, 50, 2, ixAttrNameRecBuf);
        void* ixAttrTypeRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 2, 9, "attr-type", PeterDB::TypeInt, 4, 3, ixAttrTypeRecBuf);
        void* ixFileNameRecBuf = malloc(100);
        prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator, 2, 14, "index-filename", PeterDB::TypeVarChar, 50, 4, ixFileNameRecBuf);

        // insert IX_Table records into Attributes table
        RID tidRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, tableIdRecBuf, tidRid);
        RID ixNameRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, ixAttrNameRecBuf, ixNameRid);
        RID ixAttrTypeRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, attrTypeRecBuf, ixAttrTypeRid);
        RID ixFilenameRid;
        RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, attrLenRecBuf, ixFilenameRid);


        free(tidRecBuf);
        free(ixAttrNameRecBuf);
        free(ixAttrTypeRecBuf);
        free(ixFileNameRecBuf);


        RecordBasedFileManager::instance().closeFile(tablesFileHandle);
        RecordBasedFileManager::instance().closeFile(attrsFileHandle);
        delete[] nullsIndicator;
        return 0;
    }

    // Check whether the given file exists
    static bool fileExists(const std::string &fileName) {
        struct stat stFileInfo{};

        return stat(fileName.c_str(), &stFileInfo) == 0;
    }

    RC RelationManager::deleteCatalog() {
        // check if Table table exists
        if(fileExists(tablesFileName)){
            // delete Tables file
            // delete Attributes file
            RecordBasedFileManager::instance().destroyFile(tablesFileName);
            RecordBasedFileManager::instance().destroyFile(attrsFileName);
            RecordBasedFileManager::instance().destroyFile(indicesFileName);
        }

        else{
            std::cout<<"Catalog doesn't exist"<<std::endl;
            return -1;
        }

        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        // check if catalog exists
        if(!canAccess(tableName)) {
            return -1;
        }

        if(fileExists(tablesFileName) && fileExists(attrsFileName)){

            if(fileExists(tableName)){
                std::cout<<"Table already exists"<<std::endl;
                return -1;
            }

            //create a file for the table
            RecordBasedFileManager::instance().createFile(tableName);

            unsigned char *nullsIndicator = initializeNullFieldsIndicator(tablesRecordDescriptor);

            // prepare Tables record
            void *outBuffer = malloc(1000);
            RM_ScanIterator rmsi;
            std::vector<std::string> attributeNames {"table-id"};
            scan(tablesFileName,"", PeterDB::NO_OP, nullptr, attributeNames, rmsi);
            memset(outBuffer, 0, 1000);
            RID rid;
            int idCounter = 0;
            while (rmsi.getNextTuple(rid, outBuffer) != RBFM_EOF) {
                idCounter++;
            }

            void* tablesBuffer = malloc(100);
            RID newRid;
            prepareTablesRecord(tablesRecordDescriptor.size(), nullsIndicator, idCounter,
                                tableName.length(), tableName, tableName.length(),
                                tableName, tablesBuffer);
            // insert record into Tables table
            RecordBasedFileManager::instance().openFile(tablesFileName, tablesFileHandle);
            RecordBasedFileManager::instance().insertRecord(tablesFileHandle, tablesRecordDescriptor, tablesBuffer, newRid);
            RecordBasedFileManager::instance().closeFile(tablesFileHandle);
            free(tablesBuffer);
            // prepare Attributes record
            int positionCounter = 0;
            for(auto attr: attrs){
                void* attrBuffer = malloc(1000);
                prepareAttrsRecord(attrsRecordDescriptor.size(), nullsIndicator,
                                   idCounter, attr.name.length(), attr.name, attr.type, attr.length,
                                   positionCounter, attrBuffer);

                RecordBasedFileManager::instance().openFile(attrsFileName, attrsFileHandle);
                RecordBasedFileManager::instance().insertRecord(attrsFileHandle, attrsRecordDescriptor, attrBuffer, newRid);
                RecordBasedFileManager::instance().closeFile(attrsFileHandle);
                positionCounter++;
                free(attrBuffer);
            }
            delete[] nullsIndicator;

        }
        else{
            std::cout<<"Catalog does not exist, cannot create table"<<std::endl;
            return -1;
        }
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        if(!canAccess(tableName)) {
            return -1;
        }

        // delete file
        // delete record

        //get the id of the table name from Tables
        std::string conditionAttribute = "table-name";
        PeterDB::CompOp compOp = PeterDB::EQ_OP;
        std::vector<std::string> attributeNames{"table-id"};
        RM_ScanIterator rmScanIterator1, rmScanIterator2;
        int id;
        RID rid;

        void *value = malloc(tableName.length() + sizeof(int));
        int tableNameLength = tableName.length();
        memcpy(value, &tableNameLength, sizeof(int));
        memcpy((char *) value + sizeof(int), tableName.c_str(), tableName.length());
        scan(tablesFileName, conditionAttribute, compOp, value, attributeNames, rmScanIterator1);
        free(value);


        void *outBuffer = malloc(1000);
        if (rmScanIterator1.getNextTuple(rid, outBuffer) == RBFM_EOF){
            // rmScanIterator1.close();
            // free(outBuffer);
            return -1;
        }

        // rmScanIterator1.close();

        RecordBasedFileManager::instance().openFile(tablesFileName, tablesFileHandle);
        RecordBasedFileManager::instance().deleteRecord(tablesFileHandle, tablesRecordDescriptor, rid);
        RecordBasedFileManager::instance().closeFile(tablesFileHandle);
        memcpy(&id, (char *) outBuffer + 1, sizeof(int));


        //get the attributes from the columns table using the id
        conditionAttribute = "table-id";
        attributeNames = {"column-name", "column-type"};
        value = malloc(sizeof(int));
        memcpy(value, &id, sizeof(int));
        scan(attrsFileName, conditionAttribute, compOp, value, attributeNames, rmScanIterator2);
        free(value);

        memset(outBuffer, 0, 1000);


        while (rmScanIterator2.getNextTuple(rid, outBuffer) != RBFM_EOF) {
            RecordBasedFileManager::instance().openFile(attrsFileName, attrsFileHandle);
            RecordBasedFileManager::instance().deleteRecord(attrsFileHandle, attrsRecordDescriptor, rid);
            RecordBasedFileManager::instance().closeFile(attrsFileHandle);
            memset(outBuffer, 0, 1000);
        }
        RecordBasedFileManager::instance().destroyFile(tableName);

        //delete indices of the table

        std::vector<std::tuple<std::string, std::string>> indices;
        getIndices(id, indices);
        for (auto index: indices) {
            destroyIndex(tableName, std::get<0>(index));
        }
        rmScanIterator2.close();
        free(outBuffer);
        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        attrs.clear();
        std::string conditionAttribute = "table-name";
        PeterDB::CompOp compOp = PeterDB::EQ_OP;
        std::vector<std::string> attributeNames {"table-id"};
        RM_ScanIterator rmScanIterator_table, rmScanIterator_attribute;
        int id;
        RID rid;

        //get the id of the table name
        void *value = malloc (tableName.length() + sizeof (int));
        int tableNameLength = tableName.length();
        memcpy(value, &tableNameLength, sizeof (int));
        memcpy((char*)value + sizeof (int), tableName.c_str(), tableName.length());
        scan(tablesFileName, conditionAttribute, compOp, value, attributeNames, rmScanIterator_table);
        free(value);

        void *outBuffer = malloc(1000);
        if (rmScanIterator_table.getNextTuple(rid, outBuffer) == RBFM_EOF ) {
            free(outBuffer);
            rmScanIterator_table.close();
            return -1;
        }
        rmScanIterator_table.close();

        memcpy(&id, (char*)outBuffer + 1, sizeof (int));

        //get the attributes from the columns table using the id
        conditionAttribute = "table-id";
        attributeNames = {"column-name", "column-type", "column-length"};
        value = malloc (sizeof (int));
        memcpy(value, &id, sizeof (int));
        scan(attrsFileName, conditionAttribute, compOp, value, attributeNames, rmScanIterator_attribute);
        memset(outBuffer, 0, 1000);
        while (rmScanIterator_attribute.getNextTuple(rid, outBuffer) != RBFM_EOF) {
            Attribute newAttr;
            int length = *(int*)((char*)outBuffer + 1);

            std::string name ((char*)outBuffer + 1 + sizeof (int), length);
            newAttr.name = name;
//            std::cout << "Last letter  " << *((char*)outBuffer + 1 + sizeof (int) + length - 1) << std::endl;
            int type = *(int*) ((char*)outBuffer + 1 + sizeof (int) + length);

            newAttr.type = static_cast<PeterDB::AttrType>(type);
            //std::cout << "In setting " << type << std::endl;

            int column_length = *(int*) ((char*)outBuffer + 1 + sizeof (int) + length + sizeof (int));
            newAttr.length = column_length;
            // std:: cout << "Column size " << column_length << std::endl;
            attrs.push_back(newAttr);
            memset(outBuffer, 0, 1000);
        }
        rmScanIterator_attribute.close();
        free(outBuffer);
        free(value);
        return 0;
    }

    RC RelationManager::findTableID (const std::string &tableName, int &id) {

        RM_ScanIterator rmsi;
        std::string conditionAttribute = "table-name";
        PeterDB::CompOp compOp = PeterDB::EQ_OP;
        std::vector<std::string> attributeNames{"table-id"};

        void *value = malloc(tableName.length() + sizeof(int));
        int tableNameLength = tableName.length();
        memcpy(value, &tableNameLength, sizeof(int));
        memcpy((char *) value + sizeof(int), tableName.c_str(), tableName.length());

        scan(tablesFileName, conditionAttribute, compOp, value, attributeNames, rmsi);

        RID tempRID;

        void *outBuffer = malloc(1000);
        if (rmsi.getNextTuple(tempRID, outBuffer) == RBFM_EOF){
            return -1;
        }
        memcpy(&id, (char *) outBuffer + 1, sizeof(int));

        rmsi.close();
        return 0;
    }

    void extractIndices (std::vector<std::tuple<std::string, std::string>> &indices, void *outBuffer) {
        char *dataPointer = (char*)outBuffer + 1;

        int sizeIndex = *(int*)dataPointer;
        dataPointer += sizeof (sizeIndex);

        std::string indexName(dataPointer, sizeIndex);
        dataPointer += sizeIndex;

        int sizeIndexFileName = *(int*)dataPointer;
        dataPointer += sizeof (sizeIndexFileName);

        std::string indexFileName(dataPointer, sizeIndexFileName);

        indices.push_back(std::make_tuple(indexName, indexFileName));
    }

    RC RelationManager::getIndices (const int &tableID, std::vector<std::tuple<std::string, std::string>> &indices) {
        RM_ScanIterator rmsi;
        std::string conditionAttribute = "table-id";
        PeterDB::CompOp compOp = PeterDB::EQ_OP;

        std::vector<std::string> attributeNames{"attr-name", "index-filename"};

        scan(indicesFileName, conditionAttribute, compOp, &tableID, attributeNames, rmsi);

        RID tempRID;

        void *outBuffer = malloc(1000);
        std::vector<PeterDB::Attribute > recordDescriptor;
        PeterDB::addAttribute(recordDescriptor, "attr-name", PeterDB::TypeVarChar, 50);
        PeterDB::addAttribute(recordDescriptor, "index-filename", PeterDB::TypeVarChar, 50);

        while (rmsi.getNextTuple(tempRID, outBuffer) != RBFM_EOF) {
            extractIndices (indices, outBuffer);
        }

        rmsi.close();
        free(outBuffer);
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if(!canAccess(tableName)) {
            return -1;
        }

        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        std::vector<PeterDB::Attribute > attrs;
        FileHandle fileHandle;

        if(tableName == tablesFileName)
            attrs = tablesRecordDescriptor;
        else if (tableName == attrsFileName)
            attrs = attrsRecordDescriptor;
        else
            getAttributes(tableName, attrs);

        if(rbfm.openFile(tableName, fileHandle)) return -1;
        rbfm.insertRecord(fileHandle, attrs, data, rid);
        rbfm.closeFile(fileHandle);

//        insertIndexEntries (data, rid, tableName, attrs);
        return 0;
    }

    RC RelationManager::deleteIndexEntries (const PeterDB::RID &rid, const std::string &tableName,
                                            std::vector<PeterDB::Attribute> &attrs) {
        int tableID;
        findTableID(tableName, tableID);
        std::vector<std::tuple<std::string, std::string>> indices;
        getIndices(tableID, indices);
        for (auto index: indices) {
            std::string attributeName = std::get<0>(index);
            std::string indexFileName = std::get<1>(index);

            void *attributeValue = new char[1000];
            readAttribute(tableName, rid, attributeName, attributeValue);
            //TODO: what if the attribute value is NULL ?

            int position;
            findPositionInRD(attrs, position, attributeName);

            IndexManager &im = IndexManager::instance();
            IXFileHandle ixFileHandle;
            im.openFile(indexFileName, ixFileHandle);
            im.deleteEntry(ixFileHandle, attrs[position], attributeValue, rid);
            im.closeFile(ixFileHandle);

            free (attributeValue);
        }
        return 0;
    }

    RC RelationManager::insertIndexEntries (const void *data, RID &rid, const std::string &tableName,
                           std::vector<PeterDB::Attribute> &attrs) {

        int tableID;
        findTableID(tableName, tableID);
        std::vector<std::tuple<std::string, std::string>> indices;
        getIndices(tableID, indices);

        for (auto index: indices) {
            std::string attributeName = std::get<0>(index);
            std::string indexFileName = std::get<1>(index);

            int position;
            findPositionInRD(attrs, position, attributeName);

            char *attributeValue = (char*) malloc(1000);
            RelationManager::instance().readAttribute(tableName, rid, attributeName, attributeValue);

            //TODO: what if the attribute value is NULL ?

            IndexManager &im = IndexManager::instance();
            IXFileHandle ixFileHandle;
            im.openFile(indexFileName, ixFileHandle);
            im.insertEntry(ixFileHandle, attrs[position], attributeValue + 1, rid);
            im.closeFile(ixFileHandle);

            //free(attributeValue);
        }
        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {

        if(!canAccess(tableName)) {
            return -1;
        }
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        std::vector<PeterDB::Attribute > attrs;
        FileHandle fileHandle;

        if(tableName == tablesFileName)
            attrs = tablesRecordDescriptor;
        else if (tableName == attrsFileName)
            attrs = attrsRecordDescriptor;
        else
            getAttributes(tableName, attrs);

        rbfm.openFile(tableName, fileHandle);

        //TODO: delete before deleting the record ?
        deleteIndexEntries (rid, tableName, attrs);

        rbfm.deleteRecord(fileHandle, attrs, rid);

        rbfm.closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        std::vector<PeterDB::Attribute > attrs;
        FileHandle fileHandle;
        getAttributes(tableName, attrs);
        updateEntry (rid, data, tableName, attrs);
        rbfm.openFile(tableName, fileHandle);
        rbfm.updateRecord(fileHandle, attrs, data, rid);
        rbfm.closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        std::vector<PeterDB::Attribute > attrs;
        FileHandle fileHandle;

         if(tableName == tablesFileName)
            attrs = tablesRecordDescriptor;
        else if (tableName == attrsFileName)
            attrs = attrsRecordDescriptor;
        else
            getAttributes(tableName, attrs);
        if(rbfm.openFile(tableName.c_str(), fileHandle)) return -1;
        if(rbfm.readRecord(fileHandle, attrs, rid, data)) {
            rbfm.closeFile(fileHandle);
            return -1;
        }
        rbfm.closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        rbfm.printRecord(attrs, data, out);
        return 0;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        std::vector<Attribute > recordDescriptor;
        getAttributes(tableName, recordDescriptor);
        FileHandle fileHandle;
        RecordBasedFileManager::instance().openFile(tableName, fileHandle);
        RecordBasedFileManager::instance().readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
        RecordBasedFileManager::instance().closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {

        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        FileHandle fileHandle;
        std::vector<Attribute> recordDescriptor;

        if (tableName == tablesFileName) {
            createTableRecordDescriptor(recordDescriptor);
            rbfm.openFile(tablesFileName, fileHandle);
        }
        else if (tableName == attrsFileName) {
            createAttrRecordDescriptor(recordDescriptor);
            rbfm.openFile(attrsFileName, fileHandle);
        }
        else {
            if (getAttributes(tableName, recordDescriptor))
                return -1;
            rbfm.openFile(tableName, fileHandle);
        }

        rbfm.scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.RBFM_Scan_Iterator);
        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        int retVal = RBFM_Scan_Iterator.getNextRecord(rid, data);
        if(retVal){
            return RM_EOF;
        }
        return 0;
    }

    RC RM_ScanIterator::close() { 
        RBFM_Scan_Iterator.close();
        return 0; }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    template<typename KeyType>
    void bulkLoad(const std::string &tableName, const std::string &attributeName, IXFileHandle ixFileHandle, Attribute attr){
        RM_ScanIterator rmsi;
        RID getNextRid;
        char outBuffer[PAGE_SIZE];
        std::vector<std::string> attributeNames {attributeName};
        RelationManager::instance().scan(tableName, "", PeterDB::NO_OP, nullptr, attributeNames, rmsi);

        std::vector<std::tuple<KeyType, PeterDB::RID>> combined;
        while (rmsi.getNextTuple(getNextRid, outBuffer) != RBFM_EOF){
            KeyType attrValue = *(KeyType*)outBuffer;
            combined.push_back(std::make_tuple(attrValue, getNextRid));
        }
        sort(combined.begin(), combined.end(), compareFunction<KeyType>);

        for(auto entry : combined){
            void* key = static_cast<void*>(&std::get<0>(entry));
            IndexManager::instance().insertEntry(ixFileHandle, attr, key, std::get<1>(entry));
        }

    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){

        // ix:createFile
        //ix:createFile for index
        std::string ixFilename = tableName + "_" + attributeName + ".idx";
        IndexManager::instance().createFile(ixFilename);
        IXFileHandle ixFileHandle;
        IndexManager::instance().openFile(ixFilename, ixFileHandle);

        //find table id in tables table
        int tableId = -1;
        findTableID(tableName, tableId);

        if(tableId == -1){
            std::cout<<"Table not found." <<std::endl;
            return -1;

        }
        //get attribute type
        AttrType ixAttrType;
        Attribute ixAttr;
        std::vector<PeterDB::Attribute> attrs;
        getAttributes(tableName, attrs);

        for(const auto& attr: attrs){
            if(attr.name == attributeName){
                ixAttr = attr;
                ixAttrType = attr.type;
            }
        }

        // insert into IX_Table
        void *indicesBuffer = malloc(100);
        unsigned char *nullsIndicator = initializeNullFieldsIndicator(tablesRecordDescriptor);

        prepareIxRecord(indicesRecordDescriptor.size(), nullsIndicator, tableId, attributeName.length(),
                        attributeName, ixAttrType, ixFilename.length(), ixFilename, indicesBuffer);

        // insert record into IX_Table
        RID ixRid;
        RecordBasedFileManager::instance().openFile(indicesFileName, indicesFileHandle);
        RecordBasedFileManager::instance().insertRecord(indicesFileHandle, indicesRecordDescriptor,
                                                        indicesBuffer, ixRid);
        RecordBasedFileManager::instance().closeFile(indicesFileHandle);

        // check if table is empty
        switch (ixAttrType) {
            case TypeInt:
                bulkLoad<int>(tableName, attributeName, ixFileHandle, ixAttr);
                break;

            case TypeReal:
                bulkLoad<float>(tableName, attributeName, ixFileHandle, ixAttr);
                break;

            case TypeVarChar:
                bulkLoad<char*>(tableName, attributeName, ixFileHandle, ixAttr);
                break;
        }

        IndexManager::instance().closeFile(ixFileHandle);

        return 0;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){

        //TODO: check if the index exists

        RM_ScanIterator rmsi;
        std::string conditionAttribute = "attr-name";
        PeterDB::CompOp compOp = PeterDB::EQ_OP;

        std::vector<std::string> attributeNames{"index-filename"};

        char *value = (char*)malloc (sizeof (int) + attributeName.size());
        memset(value, 0, sizeof (int) + attributeName.size());

        int attributeNameSize = attributeName.size();
        memcpy(value, &attributeNameSize, sizeof (int));
        memcpy(value + sizeof (int), attributeName.c_str(), attributeNameSize);

        scan(indicesFileName, conditionAttribute, compOp, value, attributeNames, rmsi);
        RID indexRid;

        void *outBuffer = malloc(1000);
        rmsi.getNextTuple(indexRid, outBuffer);

        char* dataPointer = (char*)outBuffer + sizeof (char);
        int indexFileNameSize = *(int*)dataPointer;
        std::string indexFileName ((char*)dataPointer + sizeof(indexFileNameSize), indexFileNameSize);

        //delete tuple and file
        deleteTuple(indicesFileName, indexRid);
        RecordBasedFileManager::instance().destroyFile(indexFileName);

        rmsi.close();
        free(outBuffer);
        return 0;
    }

    bool isSame (void *OGRecordValue, void *newRecordValue, AttrType type) {
        int newRecordSize;
        int OGRecordSize;

        switch (type) {
            case TypeVarChar: {
                OGRecordSize = *(int*)OGRecordValue;
                newRecordSize = *(int*)newRecordValue;
                std::string OGRecordString((char*)OGRecordValue + sizeof (int), OGRecordSize);
                std::string newRecordString ((char*)newRecordValue + sizeof (int), newRecordSize);
                return OGRecordString == newRecordString;
            }
            case TypeInt:
                return *(int*)OGRecordValue == *(int*)newRecordValue;

            case TypeReal:
                return *(float*)OGRecordValue == *(float*)newRecordValue;
        }
    }

    bool changeIndexes(std::tuple<std::string, std::string> &index, const RID &rid,
                        const std::string &tableName,
                        const void *data, std::vector<Attribute> &attrs) {

        std::string attributeName = std::get<0>(index);
        std::string indexFileName = std::get<1>(index);

        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        void *OGRecordValue = malloc(1000);
        RelationManager::instance().readAttribute(tableName, rid, attributeName, OGRecordValue);

        char *newRecordValue;

        int position;
        findPositionInRD (attrs, position, attributeName);

        //TODO: readSingleAttribute needs serialized record and not deserialized
        newRecordValue = (char*) malloc(1000);
        RelationManager::instance().readAttribute(tableName, rid, attributeName, newRecordValue);

        if (!isSame(OGRecordValue, newRecordValue, attrs[position].type)) {
            IndexManager &im = IndexManager::instance();
            IXFileHandle ixFileHandle;
            im.openFile(indexFileName, ixFileHandle);
            im.deleteEntry(ixFileHandle, attrs[position], OGRecordValue, rid);
            im.insertEntry(ixFileHandle, attrs[position], newRecordValue, rid);
        }
    }

    RC RelationManager::updateEntry (const RID &rid, const void *data,
                    const std::string &tableName, std::vector<Attribute> &attrs) {
        int tableID;
        findTableID(tableName, tableID);

        std::vector<std::tuple<std::string, std::string>> indices;
        //function to get a list of indices with their indexname and its filename
        //- a function which takes tableID as input
            // get attributes of index table
            //- calls scan on IX_Table
            //- condition attribute is Table ID
            //- value is ID
            //- compop is EQ_OP
            //- attributeNames are get attributes
            //- getNextTuple until EOF
            //- for each entry returned, extract the index and index file name
            //- call read single attribute on both the attributes
        getIndices (tableID, indices);

        // after getting the indices, extract that attribute in both new and old record -- function
        for (auto index: indices) {
            changeIndexes(index, rid, tableName, data, attrs);
            //compare the two, if they are different, update the index
            //update: deleteEntry and insertEntry in that index
            //open the index file and perform the modifications
        }
        return 0;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                                  const std::string &attributeName,
                                  const void *lowKey,
                                  const void *highKey,
                                  bool lowKeyInclusive,
                                  bool highKeyInclusive,
                                  RM_IndexScanIterator &rm_IndexScanIterator){

        std::string ixFilename = tableName+"_"+attrsFileName;
        IXFileHandle ixFileHandle;
        IndexManager::instance().openFile(ixFilename, ixFileHandle);
        std::vector<Attribute> attributes;
        getAttributes(tableName,attributes);
        Attribute attribute;
        for(const auto& attr:attributes){
            if(attr.name == attributeName){
                attribute = attr;
                break;
            }
        }

        IndexManager::instance().scan(ixFileHandle, attribute,lowKey, highKey,lowKeyInclusive,highKeyInclusive,
                                      rm_IndexScanIterator.IX_Scan_Iterator);
        return 0;
    }

    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return -1;
    }

    RC RM_IndexScanIterator::close(){
        return 0;
    }

} // namespace PeterDB