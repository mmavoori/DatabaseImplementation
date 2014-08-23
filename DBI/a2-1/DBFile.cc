#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

#include<iostream>
#include<stdlib.h>

// Algorithm used in filling and commiting the write buffer: (this handles all
// combinations of Add and GetNext so that there are no gaps in the pages of the file)
//
// The flag 'writeBufferCopyOfLastPage' tells if the write buffer is a copy of 
// the file's last page or a fresh buffer.
//
// GetPage() into write buffer:
//         case1: with last page of file --> set writeBufferCopyOfLastPage = 1
//         case2: a fresh buffer         --> set writeBufferCopyOfLastPage = 0
//
// AddPage() from write buffer:
//         if(writeBufferCopyOfLastPage)
//                commit to last existing page of file by overwriting it
//         else
//                commit to a fresh page after the existing last page of file

int DBFile::GetFileLength()
{
    if(file.GetLength() == 0)
        return file.GetLength();
    else
        return file.GetLength() - 1;
}

DBFile::DBFile () 
{
    // create an empty read buffer
    readPageBuffer = new (std::nothrow) Page;
    if(readPageBuffer == NULL)
    {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
    }
    
    // create an empty write buffer
    writePageBuffer = new (std::nothrow) Page;
    if(writePageBuffer == NULL)
    {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
    }
    
    writeBufferDirty = 0;
    writeBufferCopyOfLastPage = 0;
    curReadPageIndex = -2; // file.GetLength() starts at 0 and takes values 2,3,4... & not 1
                           // Actually it should start at 1 and take values 2,3,4... 
                           // (Confusing@#!$* bcoz 'O'th page has no data in a file)
}

int DBFile::Create (char *file_path, fType file_type, void *startup) 
{
    // for this assignment
    file_type = heap;
    
    file.Open(0,file_path);
    return 1;
}

int DBFile::Open (char *file_path) 
{
    file.Open(1,file_path);
    
    // Get the first page of the file, if any, into the read buffer
    if(GetFileLength() > 0) {   // means not an empty file
         curReadPageIndex = 0;
         file.GetPage(readPageBuffer, curReadPageIndex);
         recordIndex = 0;
    }
    
    // Get the last page of the file, if any, into the write buffer, because
    // we have to append records to the file without leaving any spaces.
    if(GetFileLength() > 0) {   // means not an empty file  
         file.GetPage(writePageBuffer, GetFileLength() - 1);
         writeBufferDirty = 0;
         writeBufferCopyOfLastPage = 1;
    }
    
    return 1;
}
    
int DBFile::Close () 
{
    // write to disk any unwritten buffer
    // Almost always write buffer is dirty, except in the cases where you commit
    // non-full write buffer and where you close the 
    // file without writing even a single record to the buffer
    if(writeBufferDirty) {
         if(writeBufferCopyOfLastPage == 1) {
              file.AddPage(writePageBuffer, GetFileLength() - 1);
          }
          else { 
               // Freshwrite allOtherCases: commit to a fresh page after last page.
              file.AddPage(writePageBuffer, GetFileLength());
          }
          
          writePageBuffer->EmptyItOut();
          writeBufferDirty = 0;
          writeBufferCopyOfLastPage = 0;
    }
    
    int file_pageLength = file.Close();
    delete readPageBuffer;
    delete writePageBuffer;
    return file_pageLength;
}


void DBFile::Load (Schema &f_schema, char *load_file_path) 
{
     FILE *file = fopen(load_file_path,"r");
     if(file == NULL) 
     {
             cerr<< "Opening file \""<< load_file_path <<"\" failed.\n";
             exit(1);
     }
     
     // Suck each record from the text file and add it to write buffer
     Record temp;

     int counter = 0;
	 while (temp.SuckNextRecord(&f_schema, file)) {
		Add(temp);
        counter++;
	 }
     cout<<"Loaded " << counter << " records from the text file.\n";
	 
	 fclose(file);
}

void DBFile::MoveFirst () 
{
     // Get the first page of the file into the read buffer
     readPageBuffer->EmptyItOut();
     file.GetPage(readPageBuffer,0);
     recordIndex = 0;
     curReadPageIndex = 0;
}

void DBFile::Add (Record &addme) 
{
     // add the record to the write buffer
     int appended = writePageBuffer->Append(&addme);
     
     if(!appended) {
          // Write buffer is full, so write it to the disk
          
          // Write buffer overwrite case1: 
          // if writeBufferCopyOfLastPage, we have to overwrite to the last page of the file
          // else commit it to a fresh page after the last page
          // GetFileLength() - 1 or file.GetLength - 2 gives the index of the last page of the file
          if(writeBufferCopyOfLastPage == 1) {
              file.AddPage(writePageBuffer, GetFileLength() - 1);
          }
          else { 
               // Freshwrite allOtherCases: commit to a fresh page after last page.
              file.AddPage(writePageBuffer, GetFileLength());
          }
          
          // now add the record to a fresh buffer
          writePageBuffer->EmptyItOut();
          writeBufferDirty = 0;
          writeBufferCopyOfLastPage = 0;
          
          appended = writePageBuffer->Append(&addme);
     }
     
     writeBufferDirty = 1;
}

int DBFile::GetNext (Record &fetchme) 
{
    // Logic for mixed read and writes from the disk's last page:
    // Here write buffer is not full, but we write it to the disk
    //
    // Write buffer overwrite case2: If we are reading from the last page of the file, and
    // if the write buffer is dirty: 
    // a. if writeBufferCopyOfLastPage, overwrite the dirty write buffer to the disk's last page,
    //    and read back the last page and skip all fetched records
    // b. if writeBuffer Not CopyOfLastPage, do nothing (Means write buffer has some new
    //    page. We'll handle this case in the coming if statement)
    if((curReadPageIndex == GetFileLength() - 1) && writeBufferDirty == 1) {
                         
          if(writeBufferCopyOfLastPage == 1) {
              // Overwrite the disk's last page
              file.AddPage(writePageBuffer, GetFileLength() - 1);
              
              // last page is a copy of the write buffer, which means effectively
              // still write buffer is a copy of the last page
              // Doing this bcoz write buffer is not full, but we are writing it to the disk
              writeBufferCopyOfLastPage = 1;
              writeBufferDirty = 0;
              
              // read back overwritten last page
              readPageBuffer->EmptyItOut();
              file.GetPage(readPageBuffer, GetFileLength() - 1);
              curReadPageIndex == GetFileLength() - 1;
              
              // skip all fetched records
              Record temp;
              for (int i = 0; i < recordIndex; i++) {
    			readPageBuffer->GetFirst(&temp);
    		  }
        }
    }
          
    // get the next record from the read buffer
    int got_next = readPageBuffer->GetFirst(&fetchme);
    
    if(!got_next) {
          // read buffer is empty
          
          // Check if this is the last page of the file
          //cout<< "curReadPageIndex : "<< curReadPageIndex <<endl ;
          if(curReadPageIndex == (GetFileLength() - 1)) {
               // Check if there is any uncommited dirty write buffer 
               // If yes, write it to a fresh page after last page and load the read buffer and continue reading
               // otherwise finished reading the entire file
               if(writeBufferDirty) {
                      file.AddPage(writePageBuffer, GetFileLength());
                      
                      // last page is a copy of the write buffer, which means effectively
                      // still write buffer is a copy of the last page
                      // Doing this bcoz write buffer is not full, but we are writing it to the disk
                      writeBufferCopyOfLastPage = 1;
                      writeBufferDirty = 0;
                }
                else {
                     return 0; // finished reading the entire file
                }
          }
               
          // read next page from the disk
          readPageBuffer->EmptyItOut();
          file.GetPage(readPageBuffer, ++curReadPageIndex);
          recordIndex = 0;
             
          // now get the next record from the read buffer
          got_next = readPageBuffer->GetFirst(&fetchme);
    }
    
    recordIndex++;
    return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
    ComparisonEngine comparisionEngine;
	
	// Get each record and compare
	do {
		if (GetNext(fetchme) == 0) {
			return 0;  // finished reading the entire file
		}
	} while (!comparisionEngine.Compare(&fetchme, &literal, &cnf));
	
	return 1;
}
