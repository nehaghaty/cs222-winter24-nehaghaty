#ifndef _ix_h_
#define _ix_h_


#include <vector>
#include <string>


#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute


# define IX_EOF (-1)  // end of the index scan
#define  FREE_SPACE     PAGE_SIZE - 4 -1


namespace PeterDB {
    class IX_ScanIterator;


    class IXFileHandle;


    class IndexManager {


    public:
        static IndexManager &instance();


        // Create an index file.
        RC createFile(const std::string &fileName);


        // Delete an index file.
        RC destroyFile(const std::string &fileName);


        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);


        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);


        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);


        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);


        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);


        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;


    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment


    };

// Abstract class Node
    class Node {
    public:
        int freeSpace;
        unsigned numKeys;
        char isLeaf;
        std::vector<PeterDB::RID> rids;
        std::vector<char*> varcharKeys; // Dynamic array of character pointers
        std::vector<int> intKeys;
        std::vector<float> floatKeys;

        // Constructor
        Node() = default; // Default constructor


        // Virtual destructor
        virtual ~Node() {}


        // Pure virtual functions for serialization and deserialization
        virtual void Serialize(void* data, const Attribute &attribute) = 0;
        virtual void Deserialize(const void* data, const Attribute &attribute) = 0;
        void writeNodeToFile(Attribute attribute, PageNum pageNum, IXFileHandle &ixFileHandle);
        void appendNodeToFile(Attribute attribute, IXFileHandle &ixFileHandle);
    };


    // Derived class InternalNode
    class InternalNode : public Node {
    public:
        std::vector<PageNum> children;
        // Implementations of pure virtual functions
        void Serialize(void *data, const Attribute &attribute) override;

        void Deserialize(const void* data, const Attribute &attribute) override;

        //void writeNodeToPage(void *data, PageNum pageNum, IXFileHandle &ixFileHandle) override;

        // Constructor and destructor if needed
        InternalNode();
        ~InternalNode() = default;
    };


// Derived class LeafNode
    class LeafNode : public Node {
    public:
//        std::vector<PeterDB::RID> rids; // Additional member for LeafNode
        PageNum next; // Sibling pointer
//        PageNum  prev;
//        PageNum pageNum;


        // Implementations of pure virtual functions
        void Serialize(void *data, const Attribute &attribute) override;


        void Deserialize(const void* data, const Attribute &attribute) override;

//        void writeNodeToPage(void *data, PageNum pageNum, IXFileHandle &ixFileHandle) override;

        // Constructor and destructor if needed
        LeafNode();
        ~LeafNode() = default;
    };

    class IXFileHandle {
    public:


        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;
        bool isOpen = false;
        PageNum dummyHead;
        FileHandle fileHandle;


        // Constructor
        IXFileHandle();


        // Destructor
        ~IXFileHandle();


        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
        RC iXReadPage(PageNum pageNum, void *data);                           // Get a specific page
        RC iXWritePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC iXAppendPage(const void *data);                                    // Append a specific page
        unsigned iXgetNumberOfPages();

    };

    class IX_ScanIterator {
    public:
        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        Attribute attribute;
        void *lowKey;
        void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        IXFileHandle ixFileHandle;
        int currentIndex;
        char currentPage[PAGE_SIZE];

        LeafNode leafNode;
        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    };

}// namespace PeterDB
#endif // _ix_h_
