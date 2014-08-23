#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

// DBFile is used for storing and retrieving records from the disk.
// Two pages in main memory(one for writing and one for reading) are used as buffers.
// Data transfer between memory and disk is done in pages in order to reduce the
// disk I/O costs.
//
// Reading:
//         First the read buffer is checked. If it's not empty, then a record is 
//         fetched from it. But if the read buffer is empty, then the next page 
//         from the file(on disk) is read into the read buffer(in memory) and
//         the record is fetched.
// Writing:
//         The record is added to the write buffer, where it waits to be written 
//         to the file. But if the write buffer is full, then it is written to
//         the file(on disk) and the record is added to a fresh empty write buffer.

class DBFile {
      
private:
    File file;
    Page *writePageBuffer;   // write buffer in memory
    Page *readPageBuffer;    // read buffer in memory
    
    off_t curReadPageIndex;  // which page from the file are we currently reading
    int recordIndex;         // Index of the record, in the read buffer, that we are currently reading
    
    bool writeBufferDirty;   // flags associated with write buffer
    bool writeBufferCopyOfLastPage;
    
    int GetFileLength();     // Function that returns correct filelength in pages

public:
	DBFile (); 

    // create a new file
	int Create (char *file_path, fType file_type, void *startup);
	
	// open a file that already exists
	int Open (char *file_path);
	
	// close the file
	int Close ();

    // Load relation data from a text file and create the binary file equivalent
	void Load (Schema &myschema, char *load_file_path);

    // Move to the first record in the file
	void MoveFirst ();
	
	// First add the record to the write buffer.
    // If the write buffer is full, write it to the disk, empty it out and
    // then add the record to it.
	void Add (Record &addme);
	
	// Fetch the next record from the read buffer.
    // IF the read buffer is empty, then fill it with the next page from the disk
    // and then fetch the record from it.
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};

#endif
