#include <cassert>
#include "src/include/ix.h"

typedef unsigned    NumKeysLength;
typedef unsigned    FreeSpaceLength;
typedef unsigned    SiblingPtrLength;
typedef char        isLeafIndicator;

#define FREE_SPACE_BYTES    sizeof(FreeSpaceLength)
#define NUM_KEYS_BYTES      sizeof(NumKeysLength)
#define IS_LEAF_BYTES       sizeof(isLeafIndicator)
#define SIBLING_BYTES       sizeof(SiblingPtrLength)

#define FREE_SPACE_OFFSET   (PAGE_SIZE - FREE_SPACE_BYTES - 1)
#define NUM_KEYS_OFFSET     (FREE_SPACE_OFFSET - NUM_KEYS_BYTES)
#define IS_LEAF_OFFSET      (NUM_KEYS_OFFSET - IS_LEAF_BYTES)
#define SIB_PTR             (IS_LEAF_OFFSET - SIBLING_BYTES)

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    RC IXFileHandle::iXReadPage(PageNum pageNum, void *data) {
        if(fileHandle.readPage(pageNum, data)){
            return -1;
        }
        ixReadPageCounter++;
        return 0;
    }
    RC IXFileHandle::iXWritePage(PageNum pageNum, const void *data) {
        if(fileHandle.writePage(pageNum, data)) {
            return -1;
        }
        ixWritePageCounter++;
        return 0;
    }

    RC IXFileHandle::iXAppendPage(const void *data) {
        if (fileHandle.appendPage(data)) {
            return -1;
        }
        ixAppendPageCounter++;
        return 0;
    }

    unsigned IXFileHandle::iXgetNumberOfPages() {
        return ixAppendPageCounter;
    }

    LeafNode::LeafNode(){
        next = -1;
        numKeys = 0;
        freeSpace = PAGE_SIZE - FREE_SPACE_BYTES - NUM_KEYS_BYTES - IS_LEAF_BYTES - SIBLING_BYTES;
        isLeaf = 1;
    }

    void createBPlusTree(IXFileHandle &iXFileHandle, LeafNode &leafNode, const Attribute &attribute) {
        leafNode.isLeaf = 1;
        iXFileHandle.dummyHead = 0;
        void* sePage[PAGE_SIZE];
        memset(sePage, 0, PAGE_SIZE);
        leafNode.Serialize(sePage, attribute);
        iXFileHandle.iXAppendPage(sePage);
        void * readbuf = malloc(PAGE_SIZE);
        iXFileHandle.iXReadPage(0, readbuf);
        int freeSpace = *(int*)((char*) readbuf+FREE_SPACE_OFFSET);
    }

    RC getLeafEntrySize (Attribute attribute, int &size, char *key) {
        size = sizeof (unsigned) + sizeof(short);

        if (attribute.type == TypeVarChar)
            size += sizeof (int) + *(int*)key;
        else
            size += sizeof (int);

        return 0;
    }

    void pushKeyToLeafVector (Attribute attribute, const void *key, LeafNode &leafNode) {
        int sizeOffset;
        switch (attribute.type) {
            case TypeInt:
                leafNode.intKeys.push_back(*(int *) key);
                sizeOffset = sizeof(int);
                break;
            case TypeReal:
                leafNode.floatKeys.push_back(*(float *) key);
                sizeOffset = sizeof(float);
                break;
            case TypeVarChar:
                leafNode.varcharKeys.push_back((char *) key);
                int keyLen = *(int *) key; // Length of VarChar key
                sizeOffset = sizeof(int) + keyLen; // Additional size for VarChar length
                break;
        }

        // Update free space for all types
        leafNode.freeSpace -= (sizeOffset + sizeof(unsigned ) + sizeof(short));
    }
    void pushKeyToInternalVector (Attribute attribute, const void *key, LeafNode &leafNode) {
    }
    void splitLeafNodes (LeafNode &leafNode1, LeafNode &leafNode2,
                         IXFileHandle &iXFileHandle, Attribute attribute) {
        PageNum newLeafNodePage = iXFileHandle.iXgetNumberOfPages();
        leafNode1.next = newLeafNodePage;
        switch (attribute.type) {
            case TypeInt:

                break;
            case TypeReal:
                break;
            case TypeVarChar:
                break;
        }
    }

    void insert(PageNum rootPtr, PageNum newChildEntry, char *smallestKey,
                    const void *key, const RID &rid, IXFileHandle &iXFileHandle,
                    const Attribute &attribute) {
        // Load desPage of root
        char desPage[PAGE_SIZE];
        char sePage[PAGE_SIZE];
        memset(sePage, 0, PAGE_SIZE);
        memset(desPage, 0, PAGE_SIZE);

        iXFileHandle.iXReadPage(0, desPage);
        char isLeaf = *(desPage + IS_LEAF_OFFSET);

        if (isLeaf) {
            LeafNode leafNode;
            leafNode.Deserialize(desPage, attribute);

            int LeafEntryLength;
            getLeafEntrySize(attribute, LeafEntryLength, (char*)key);

            if (leafNode.freeSpace >= LeafEntryLength) {
                // if a leaf has space, we don't care if it's a root or not
                leafNode.rids.push_back(rid);
                leafNode.numKeys += 1;

                //TODO: search for the index to insert, currently inserting to the end
                pushKeyToLeafVector (attribute, key, leafNode);

                // Serialize and write back
                leafNode.Serialize(sePage, attribute);
                iXFileHandle.iXWritePage(rootPtr, sePage);
                newChildEntry = -1;
                smallestKey = nullptr;
            }
            else {
                //but if it doesn't have space, we respect the root and treat it differently
                LeafNode newLeafNode;
                pushKeyToLeafVector(attribute, key, leafNode);
                splitLeafNodes(leafNode, newLeafNode, iXFileHandle, attribute);
            }
        }

    }

    int findKey (LeafNode leafNode, const void *searchKey, const Attribute &attribute) {
        if(attribute.type == PeterDB::TypeInt){
            for (int i = 0; i < leafNode.intKeys.size(); i++) {
                if(leafNode.intKeys[i] >= *(int*)searchKey){
                    return i;
                }
            }
        }
        else if(attribute.type == PeterDB::TypeReal){
            for (int i = 0; i < leafNode.floatKeys.size(); i++) {
                if(leafNode.floatKeys[i] >= *(float*)searchKey){
                    return i;
                }
            }
        }
        else{
            for (int i = 0; i < leafNode.varcharKeys.size(); i++) {
                int keyLen = *(int*)leafNode.varcharKeys[i];
                char keyVal[keyLen];
                memcpy(keyVal, leafNode.varcharKeys[i] + sizeof(keyLen), keyLen);
                std::cout << keyVal[0] << " " << keyVal[keyLen-1] << std::endl;
                std::cout << "While finding " << keyLen << std::endl;
                if (memcmp(leafNode.varcharKeys[i], searchKey, keyLen + sizeof (keyLen)) == 0)
                    return i;
            }
        }

        return -1;
    }

    int search(PageNum rootPtr, const void* key, IXFileHandle &iXFileHandle, const Attribute &attribute) {
        void* desPage[PAGE_SIZE];
        memset(desPage, 0, PAGE_SIZE);
        iXFileHandle.iXReadPage(rootPtr, desPage);
        int returnedIndex = -1;

        if(* ((char*)desPage + IS_LEAF_OFFSET)){
            LeafNode leafNode;
            leafNode.Deserialize(desPage, attribute);
            returnedIndex = findKey(leafNode, key, attribute);
        }
        return returnedIndex;
    }

    template<typename KeyType>
    void copyKeys(char*& offsetPointer, const std::vector<KeyType>& keys, const std::vector<RID>& rids, const std::vector<PageNum>& children) {
        assert(keys.size() + 1 == children.size() || children.empty()); // Ensure children size is correct for internal nodes

        for (size_t i = 0; i < keys.size(); ++i) {
            // Handle keys
            if (keys[i] == -1)
                continue;

            memcpy(offsetPointer, &keys[i], sizeof(KeyType));
            offsetPointer += sizeof(KeyType);

            // Copy RIDs for leaf nodes
            if (!rids.empty()) {
                memcpy(offsetPointer, &rids[i].pageNum, sizeof(rids[i].pageNum));
                offsetPointer += sizeof(rids[i].pageNum);
                memcpy(offsetPointer, &rids[i].slotNum, sizeof (rids[i].slotNum));
                offsetPointer += sizeof(rids[i].slotNum);
            }

            // Copy children page numbers for internal nodes
            if (!children.empty()) {
                memcpy(offsetPointer, &children[i], sizeof(PageNum));
                offsetPointer += sizeof(PageNum);
            }
        }

        // For internal nodes, also copy the last child
        if (!children.empty()) {
            memcpy(offsetPointer, &children.back(), sizeof(PageNum));
            offsetPointer += sizeof(PageNum);
        }
    }


    template<>
    void copyKeys<char*>(char*& offsetPointer, const std::vector<char*>& keys, const std::vector<RID>& rids,
                         const std::vector<PageNum>& children) {
        for (size_t i = 0; i < keys.size(); ++i) {
            if (keys[i] == nullptr)
                continue;

            int keyLen = *(int*)keys[i];
            memcpy(offsetPointer, &keyLen, sizeof(int));
            offsetPointer += sizeof(int);
            memcpy(offsetPointer, keys[i] + sizeof(int), keyLen);
            offsetPointer += keyLen;

            // Copy RIDs for leaf nodes
            if (!rids.empty()) {
                memcpy(offsetPointer, &rids[i].pageNum, sizeof(rids[i].pageNum));
                offsetPointer += sizeof(rids[i].pageNum);
                memcpy(offsetPointer, &rids[i].slotNum, sizeof (rids[i].slotNum));
                offsetPointer += sizeof(rids[i].slotNum);
            }

            // Copy children page numbers for internal nodes
            if (!children.empty() && i < children.size()) {
                memcpy(offsetPointer, &children[i], sizeof(PageNum));
                offsetPointer += sizeof(PageNum);
            }
        }

        // For internal nodes, also copy the last child
        if (!children.empty()) {
            memcpy(offsetPointer, &children.back(), sizeof(PageNum));
            offsetPointer += sizeof(PageNum);
        }
    }

    static void copyVarcharKeys (char* offsetPointer, std::vector<char*> &keys,
                                    std::vector<RID> &rids, std::vector<PageNum> &children) {
        for(int i = 0 ; i < keys.size() ; i++){
            if (keys[i] == nullptr)
                continue;

            int keyLen = *(int*)keys[i];
            memcpy(offsetPointer, &keyLen, sizeof(keyLen));
            offsetPointer += sizeof(keyLen);
            memcpy(offsetPointer, keys[i] + sizeof(keyLen),keyLen);
            offsetPointer += keyLen;
            memcpy(offsetPointer, &rids[i].pageNum, sizeof(rids[i].pageNum));
            offsetPointer += sizeof(rids[i].pageNum);
            memcpy(offsetPointer, &rids[i].slotNum, sizeof (rids[i].slotNum));
            offsetPointer += sizeof(rids[i].slotNum);
        }
    }

    static void copyIntKeys (char* offsetPointer, std::vector<int> &keys,
                                    std::vector<RID> &rids, std::vector<PageNum> &children) {
        for(int i = 0 ; i < keys.size() ; i++){
            if (keys[i] == -1)
                continue;

            memcpy(offsetPointer, &keys[i],sizeof(keys[i]));
            offsetPointer += sizeof (keys[i]);
            memcpy(offsetPointer, &rids[i].pageNum, sizeof(rids[i].pageNum));
            offsetPointer += sizeof(rids[i].pageNum);
            memcpy(offsetPointer, &rids[i].slotNum, sizeof (rids[i].slotNum));
            offsetPointer += sizeof(rids[i].slotNum);
        }
    }

    static void copyFloatKeys (char* offsetPointer, std::vector<float> &keys,
                                    std::vector<RID> &rids, std::vector<PageNum> &children) {
        for(int i = 0 ; i < keys.size() ; i++){
            if (keys[i] == -1)
                continue;

            memcpy(offsetPointer, &keys[i],sizeof(keys[i]));
            offsetPointer += sizeof (keys[i]);
            memcpy(offsetPointer, &rids[i].pageNum, sizeof(rids[i].pageNum));
            offsetPointer += sizeof(rids[i].pageNum);
            memcpy(offsetPointer, &rids[i].slotNum, sizeof (rids[i].slotNum));
            offsetPointer += sizeof(rids[i].slotNum);
        }
    }

    static void populateVarcharKeys (char* offsetPointer, std::vector<char*> &keys,
                                std::vector<RID> &rids, int numKeys) {
        for (int i = 0; i < numKeys; ++i) {
            int keyLen;
            memcpy(&keyLen, offsetPointer, sizeof(keyLen));
            char *key = offsetPointer;
            offsetPointer += keyLen + sizeof(keyLen);

            RID rid;
            memcpy(&rid.pageNum, offsetPointer, sizeof(rid.pageNum));
            offsetPointer += sizeof(rid.pageNum);
            memcpy(&rid.slotNum, offsetPointer, sizeof(rid.slotNum));
            offsetPointer += sizeof(rid.slotNum);

            // Add the key and RID to the vectors
            keys.push_back(key);
            rids.push_back(rid);
        }
    }

    static void populateFloatKeys (char* offsetPointer, std::vector<float> &keys,
                                std::vector<RID> &rids, int numKeys) {
        for (int i = 0; i < numKeys; ++i) {
            float key = *(float*)offsetPointer;
            offsetPointer += sizeof(key);

            RID rid;
            memcpy(&rid.pageNum, offsetPointer, sizeof(rid.pageNum));
            offsetPointer += sizeof(rid.pageNum);
            memcpy(&rid.slotNum, offsetPointer, sizeof(rid.slotNum));
            offsetPointer += sizeof(rid.slotNum);

            // Add the key and RID to the vectors
            keys.push_back(key);
            rids.push_back(rid);
        }
    }

    static void populateIntKeys (char* offsetPointer, std::vector<int> &keys,
                                std::vector<RID> &rids, int numKeys) {
        for (int i = 0; i < numKeys; ++i) {
            int key = *(int*)offsetPointer;
            offsetPointer += sizeof(key);

            RID rid;
            memcpy(&rid.pageNum, offsetPointer, sizeof(rid.pageNum));
            offsetPointer += sizeof(rid.pageNum);
            memcpy(&rid.slotNum, offsetPointer, sizeof(rid.slotNum));
            offsetPointer += sizeof(rid.slotNum);

            // Add the key and RID to the vectors
            keys.push_back(key);
            rids.push_back(rid);
        }
    }

//    void LeafNode::Serialize(void* data, const Attribute &attribute) {
//        // Write the entries to page
//        *(int*)     ((char*)data + FREE_SPACE_OFFSET)  = freeSpace;
//        *(int*)     ((char*)data + NUM_KEYS_OFFSET)    = numKeys;
//        *(char*)    ((char*)data + IS_LEAF_OFFSET)     = isLeaf;
//        *(PageNum*) ((char*)data + SIB_PTR)            = next;
//        char* offsetPointer = (char*)data;
//        std::vector<PageNum> children;
//
//        switch (attribute.type) {
//            case TypeVarChar:
//                copyVarcharKeys(offsetPointer, varcharKeys, rids, children);
//                break;
//            case TypeReal:
//                copyFloatKeys(offsetPointer, floatKeys, rids, children);
//                break;
//            case TypeInt:
//                copyIntKeys(offsetPointer, intKeys, rids, children);
//                break;
//        }
//    }

    template<typename KeyType>
    void populateKeys(char*& offsetPointer, std::vector<KeyType>& keys, std::vector<RID>& rids,
                      std::vector<PageNum>& children, int numKeys, bool isLeaf) {
        for (int i = 0; i < numKeys; ++i) {
            KeyType key;
            memcpy(&key, offsetPointer, sizeof(KeyType));
            offsetPointer += sizeof(KeyType);
            keys.push_back(key);

            // Populate RIDs for leaf nodes
            if (!isLeaf) {
                RID rid;
                memcpy(&rid.pageNum, offsetPointer, sizeof(rid.pageNum));
                offsetPointer += sizeof(rid.pageNum);
                memcpy(&rid.slotNum, offsetPointer, sizeof(rid.slotNum));
                offsetPointer += sizeof(rid.slotNum);
            }

            // For internal nodes, there's a child associated with every key
            if (!isLeaf) { // Don't read past the keys for children
                PageNum childPage;
                memcpy(&childPage, offsetPointer, sizeof(PageNum));
                offsetPointer += sizeof(PageNum);
                children.push_back(childPage);
            }
        }

        // For internal nodes, get the last child page number
        if (!children.empty()) {
            PageNum childPage;
            memcpy(&childPage, offsetPointer, sizeof(PageNum));
            offsetPointer += sizeof(PageNum);
            children.push_back(childPage);
        }
    }

    template<>
    void populateKeys<char*>(char*& offsetPointer, std::vector<char*>& keys, std::vector<RID>& rids,
                             std::vector<PageNum>& children, int numKeys, bool isLeaf) {
        for (int i = 0; i < numKeys; ++i) {
            int keyLen;
            memcpy(&keyLen, offsetPointer, sizeof(int));
            keys.push_back(offsetPointer);

            offsetPointer += sizeof(int) + keyLen;


            // Populate RIDs for leaf nodes
            if(isLeaf){
                RID rid;
                memcpy(&rid.pageNum, offsetPointer, sizeof(rid.pageNum));
                offsetPointer += sizeof(rid.pageNum);
                memcpy(&rid.slotNum, offsetPointer, sizeof(rid.slotNum));
                offsetPointer += sizeof(rid.slotNum);
            }

            // For internal nodes, there's a child associated with every key
            if (!isLeaf) {
                PageNum childPage;
                memcpy(&childPage, offsetPointer, sizeof(PageNum));
                offsetPointer += sizeof(PageNum);
                children.push_back(childPage);
            }
        }

        // For internal nodes, get the last child page number
        if (!children.empty()) {
            PageNum childPage;
            memcpy(&childPage, offsetPointer, sizeof(PageNum));
            offsetPointer += sizeof(PageNum);
            children.push_back(childPage);
        }
    }


    void LeafNode::Serialize(void* data, const Attribute &attribute) {
        *(int*)((char*)data + FREE_SPACE_OFFSET) = freeSpace;
        *(int*)((char*)data + NUM_KEYS_OFFSET) = numKeys;
        *(char*)((char*)data + IS_LEAF_OFFSET) = isLeaf;
        char* offsetPointer = (char*)data ; // Advance past header

        switch (attribute.type) {
            case TypeInt:
                copyKeys<int>(offsetPointer, intKeys, rids, std::vector<PageNum>()); // Empty vector for PageNum
                break;
            case TypeReal:
                copyKeys<float>(offsetPointer, floatKeys, rids, std::vector<PageNum>());
                break;
            case TypeVarChar:
                copyKeys<char*>(offsetPointer, varcharKeys, rids, std::vector<PageNum>());
                break;
        }
    }

    void LeafNode::Deserialize(const void* data, const Attribute &attribute) {
        freeSpace = *(int*)((char*)data + FREE_SPACE_OFFSET);
        numKeys = *(int*)((char*)data + NUM_KEYS_OFFSET);
        isLeaf = *(char*)((char*)data + IS_LEAF_OFFSET);
        char* offsetPointer = (char*)data;
        std::vector<PageNum> emptyChildren;

//        // Clear existing data
//        intKeys.clear();
//        floatKeys.clear();
//        varcharKeys.clear();
//        rids.clear();

        switch (attribute.type) {
            case TypeInt:
                populateKeys<int>(offsetPointer, intKeys, rids, emptyChildren, numKeys, true);
                break;
            case TypeReal:
                populateKeys<float>(offsetPointer, floatKeys, rids,emptyChildren, numKeys, true);
                break;
            case TypeVarChar:
                populateKeys<char*>(offsetPointer, varcharKeys, rids, emptyChildren, numKeys, true);
                break;
        }
    }

//    void LeafNode::Deserialize(const void* data, const Attribute &attribute) {
//        // Read the entries from page
//        freeSpace   = *(int*)     ((char*)data + FREE_SPACE_OFFSET);
//        numKeys     = *(int*)     ((char*)data + NUM_KEYS_OFFSET);
//        isLeaf      = *(char*)    ((char*)data + IS_LEAF_OFFSET);
//        next        = *(PageNum*) ((char*)data + SIB_PTR);
//
//        char* offsetPointer = (char*)data;
//        switch (attribute.type) {
//            case TypeVarChar:
//                populateVarcharKeys (offsetPointer, varcharKeys, rids, numKeys);
//                break;
//            case TypeReal:
//                populateFloatKeys (offsetPointer, floatKeys, rids, numKeys);
//                break;
//            case TypeInt:
//                populateIntKeys (offsetPointer, intKeys, rids, numKeys);
//                break;
//        }
//    }

    static void copyVarcharKeysIN (char* offsetPointer, std::vector<char*> &keys, std::vector<PageNum> &children) {

        assert(keys.size()+1 == children.size());

        for(int i = 0 ; i < keys.size() ; i++) {

            memcpy(offsetPointer, &children[i], sizeof(PageNum));
            offsetPointer += sizeof(PageNum);
            int keyLen = *(int *) keys[i];
            memcpy(offsetPointer, &keyLen, sizeof(keyLen));
            offsetPointer += sizeof(keyLen);
            memcpy(offsetPointer, keys[i] + sizeof(keyLen), keyLen);
            offsetPointer += keyLen;
        }
        memcpy(offsetPointer, &children[children.size()-1], sizeof (PageNum));
    }

    static void copyIntKeysIN (char* offsetPointer, std::vector<int> &keys, std::vector<PageNum> &children) {

        assert(keys.size()+1 == children.size());

        for(int i = 0 ; i < keys.size() ; i++){

            memcpy(offsetPointer, &children[i], sizeof(PageNum));
            offsetPointer += sizeof(PageNum);
            memcpy(offsetPointer, &keys[i],sizeof(keys[i]));
            offsetPointer += sizeof (keys[i]);
        }
        memcpy(offsetPointer, &children[children.size()-1], sizeof (PageNum));
    }

    static void copyFloatKeysIN (char* offsetPointer, std::vector<float> &keys, std::vector<PageNum> &children) {

        assert(keys.size()+1 == children.size());
        for(int i = 0 ; i < keys.size() ; i++){

            memcpy(offsetPointer, &children[i], sizeof(PageNum));
            offsetPointer += sizeof(PageNum);
            memcpy(offsetPointer, &keys[i],sizeof(keys[i]));
            offsetPointer += sizeof (keys[i]);
        }
        memcpy(offsetPointer, &children[children.size()-1], sizeof (PageNum));
    }

    static void populateVarcharKeysIN (char* offsetPointer, std::vector<char*> &keys,
                                   std::vector<PageNum > &children, int numKeys) {
        for (int i = 0; i < numKeys; ++i) {

            children.push_back(*(int*)offsetPointer);
            offsetPointer += sizeof(PageNum);

            int keyLen;
            memcpy(&keyLen, offsetPointer, sizeof(keyLen));
            char *key = offsetPointer;
            keys.push_back(key);
            offsetPointer += keyLen + sizeof(keyLen);
        }
        children.push_back(*(int*)offsetPointer);
    }

    static void populateFloatKeysIN (char* offsetPointer, std::vector<float> &keys,
                                   std::vector<PageNum > &children, int numKeys) {
        for (int i = 0; i < numKeys; ++i) {

            children.push_back(*(int*)offsetPointer);
            offsetPointer += sizeof(PageNum);

            float key = *(float*)offsetPointer;
            keys.push_back(key);
            offsetPointer += sizeof(key);

        }
        children.push_back(*(int*)offsetPointer);
    }

    static void populateIntKeysIN (char* offsetPointer, std::vector<int> &keys,
                                   std::vector<PageNum > &children, int numKeys) {
        for (int i = 0; i < numKeys; ++i) {

            children.push_back(*(int*)offsetPointer);
            offsetPointer += sizeof(PageNum);

            int key = *(int*)offsetPointer;
            keys.push_back(key);
            offsetPointer += sizeof(key);

        }
        children.push_back(*(int*)offsetPointer);
    }

//    void InternalNode::Serialize(void* data, const Attribute &attribute) {
//        // Write the entries to page
//        *(int*)     ((char*)data + FREE_SPACE_OFFSET)  = freeSpace;
//        *(int*)     ((char*)data + NUM_KEYS_OFFSET)    = numKeys;
//        *(char*)    ((char*)data + IS_LEAF_OFFSET)     = isLeaf;
//        std::vector<PeterDB::RID> rids;
//
//        char* offsetPointer = (char*)data;
//
//        switch (attribute.type) {
//            case TypeVarChar:
//                copyVarcharKeysIN(offsetPointer, varcharKeys, children);
//                break;
//            case TypeReal:
//                copyFloatKeysIN(offsetPointer, floatKeys,  children);
//                break;
//            case TypeInt:
//                copyIntKeysIN(offsetPointer, intKeys,children);
//                break;
//        }
//    }

//    void InternalNode::Serialize(void* data, const Attribute &attribute) {
//        // Preset header data
//        *(int*)((char*)data + FREE_SPACE_OFFSET) = freeSpace;
//        *(int*)((char*)data + NUM_KEYS_OFFSET) = numKeys;
//        *(char*)((char*)data + IS_LEAF_OFFSET) = isLeaf;  // For internal nodes, this should typically be set to false.
//
//        char* offsetPointer = (char*)data;
//
//        // Depending on the type, serialize keys and children page numbers
//        switch (attribute.type) {
//            case TypeInt:
//                copyKeys<int>(offsetPointer, intKeys, std::vector<RID>(), children); // No RIDs for internal nodes
//                break;
//            case TypeReal:
//                copyKeys<float>(offsetPointer, floatKeys, std::vector<RID>(), children); // No RIDs for internal nodes
//                break;
//            case TypeVarChar:
//                copyKeys<char*>(offsetPointer, varcharKeys, std::vector<RID>(), children); // No RIDs for internal nodes
//                break;
//            default:
//                // Error handling or default case
//                break;
//        }
//    }
//
//    void InternalNode::Deserialize(const void* data, const Attribute &attribute) {
//        // Read the entries from page
//        freeSpace   = *(int*)     ((char*)data + FREE_SPACE_OFFSET);
//        numKeys     = *(int*)     ((char*)data + NUM_KEYS_OFFSET);
//        isLeaf      = *(char*)    ((char*)data + IS_LEAF_OFFSET);
//
//
//        char* offsetPointer = (char*)data;
//        switch (attribute.type) {
//            case TypeVarChar:
//                populateVarcharKeysIN (offsetPointer, varcharKeys, children, numKeys);
//                break;
//            case TypeReal:
//                populateFloatKeysIN (offsetPointer, floatKeys, children, numKeys);
//                break;
//            case TypeInt:
//                populateIntKeysIN (offsetPointer, intKeys, children, numKeys);
//                break;
//        }
//    }

    void InternalNode::Serialize(void* data, const Attribute &attribute) {
        *(int*)((char*)data + FREE_SPACE_OFFSET) = freeSpace;
        *(int*)((char*)data + NUM_KEYS_OFFSET) = numKeys;
        *(char*)((char*)data + IS_LEAF_OFFSET) = isLeaf;
        char* offsetPointer = (char*)data; // Advance past header

        // Note: Internal nodes do not have RIDs
        switch (attribute.type) {
            case TypeInt:
                copyKeys<int>(offsetPointer, intKeys, std::vector<RID>(), children);
                break;
            case TypeReal:
                copyKeys<float>(offsetPointer, floatKeys, std::vector<RID>(), children);
                break;
            case TypeVarChar:
                copyKeys<char*>(offsetPointer, varcharKeys, std::vector<RID>(), children);
                break;
        }
    }

    // InternalNode::Deserialize
    void InternalNode::Deserialize(const void* data, const Attribute &attribute) {
        freeSpace = *(int*)((char*)data + FREE_SPACE_OFFSET);
        numKeys = *(int*)((char*)data + NUM_KEYS_OFFSET);
        isLeaf = *(char*)((char*)data + IS_LEAF_OFFSET);
        char* offsetPointer = (char*)data;
        std::vector<RID> emptyRids;


        // Note: Internal nodes do not have RIDs
        switch (attribute.type) {
            case TypeInt:
                populateKeys<int>(offsetPointer, intKeys, emptyRids, children, numKeys, false);
                break;
            case TypeReal:
                populateKeys<float>(offsetPointer, floatKeys, emptyRids, children, numKeys, false);
                break;
            case TypeVarChar:
                populateKeys<char*>(offsetPointer, varcharKeys, emptyRids, children, numKeys, false);
                break;
        }
    }

    RC IndexManager::createFile(const std::string &fileName) {
        if(PagedFileManager::instance().createFile(fileName)){
            return -1;
        }
        return 0;
    }


    RC IndexManager::destroyFile(const std::string &fileName) {
        if(PagedFileManager::instance().destroyFile(fileName)){
            return -1;
        }
        return 0;
    }


    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &iXFileHandle) {
        if(iXFileHandle.isOpen){
            std::cout << "File already opened" <<std::endl;
            return -1;
        }
        if(PagedFileManager::instance().openFile(fileName, iXFileHandle.fileHandle)){
            return -1;
        }
        iXFileHandle.isOpen = true;
        return 0;
    }


    RC IndexManager::closeFile(IXFileHandle &iXFileHandle) {
        if(PagedFileManager::instance().closeFile(iXFileHandle.fileHandle)){
            return -1;
        }
        iXFileHandle.isOpen = false;
        return 0;
    }

    RC
    IndexManager::insertEntry(IXFileHandle &iXFileHandle, const Attribute &attribute, const void *key, const RID &rid) {

        if(iXFileHandle.dummyHead == -1){
            // create new b plus tree
            LeafNode leafNode;
            createBPlusTree(iXFileHandle, leafNode, attribute);
        }
        int newChildPageNum;
        char *smallestKey;
        insert(iXFileHandle.dummyHead, newChildPageNum, smallestKey,
                    key, rid, iXFileHandle, attribute);

        std::cout << "Searched key " << search(iXFileHandle.dummyHead, key, iXFileHandle, attribute) << std::endl;
        return 0;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &iXFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }


    RC IndexManager::scan(IXFileHandle &iXFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return -1;
    }


    RC IndexManager::printBTree(IXFileHandle &iXFileHandle, const Attribute &attribute, std::ostream &out) const {
    }


    IX_ScanIterator::IX_ScanIterator() {
    }


    IX_ScanIterator::~IX_ScanIterator() {
    }


    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }


    RC IX_ScanIterator::close() {
        return -1;
    }


    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
        dummyHead = -1;
    }


    IXFileHandle::~IXFileHandle() {
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return -1;
    }
} // namespace PeterDB
