#include <merge.h>
#include <stdio.h>
#include "chunk.h"
#include <string.h>
#include <stdlib.h>
CHUNK_Iterator CHUNK_CreateIterator(int fileDesc, int blocksInChunk)
{
    CHUNK_Iterator iterator;
    iterator.file_desc = fileDesc;
    iterator.current = 1;                                  // Start from block 1
    iterator.lastBlocksID = HP_GetIdOfLastBlock(fileDesc); // Initial value, update as needed
    iterator.blocksInChunk = blocksInChunk;
    return iterator;
}

int CHUNK_GetNext(CHUNK_Iterator *iterator, CHUNK *chunk)
{
    // Check if the current block exceeds the specified range
    if (iterator->current > iterator->lastBlocksID)
    {
        return -1; // No more CHUNKs to retrieve
    }
    // Set CHUNK parameters based on iterator values
    chunk->file_desc = iterator->file_desc;
    chunk->from_BlockId = iterator->current;
    chunk->to_BlockId = iterator->current + iterator->blocksInChunk - 1;
    if (chunk->to_BlockId > HP_GetIdOfLastBlock(iterator->file_desc)) // added this line
    {
        chunk->to_BlockId = HP_GetIdOfLastBlock(iterator->file_desc);
    } // till here
    int i;
    int total_records = 0;
    for (i = chunk->from_BlockId; i <= chunk->to_BlockId; i++) // added this
    {
        total_records = total_records + HP_GetRecordCounter(chunk->file_desc, i);
    }                                      // till here
    chunk->recordsInChunk = total_records; // You may need to update this based on your requirements
    chunk->blocksInChunk = iterator->blocksInChunk;

    // Move to the next CHUNK
    iterator->current += iterator->blocksInChunk;

    return 0; // Success
}

// Function to retrieve the ith record from a CHUNK
int CHUNK_GetIthRecordInChunk(CHUNK *chunk, int i, Record *record)
{
    int totalRecords = 0;

    // Iterate through each block in the chunk
    for (int blockId = chunk->from_BlockId; blockId <= chunk->to_BlockId; ++blockId)
    {

        // Get the number of records in the current block
        int recordsInBlock = HP_GetRecordCounter(chunk->file_desc, blockId);

        // Check if the ith record is within the current block's range
        if (i < totalRecords + recordsInBlock)
        {
            // Calculate the cursor within the block for the ith record
            int cursor = i - totalRecords;

            // Use HP_GetRecord to retrieve the record
            int result = HP_GetRecord(chunk->file_desc, blockId, cursor, record);
            HP_Unpin(chunk->file_desc, blockId); // added this
            return result;
        }

        // Update the totalRecords counter for the next block
        totalRecords += recordsInBlock;
    }

    // If the function reaches here, the ith record is not found within the chunk
    return -1; // Or handle the case as appropriate for your application
}

// Function to update the ith record in a CHUNK
int CHUNK_UpdateIthRecord(CHUNK *chunk, int i, Record record)
{
    int totalRecords = 0;

    // Iterate through each block in the chunk
    for (int blockId = chunk->from_BlockId; blockId <= chunk->to_BlockId; ++blockId)
    {
        // Get the number of records in the current block
        int recordsInBlock = HP_GetRecordCounter(chunk->file_desc, blockId);

        // Check if the ith record is within the current block's range
        if (i < totalRecords + recordsInBlock)
        {
            // Calculate the cursor within the block for the ith record
            int cursor = i - totalRecords;

            // Use HP_UpdateRecord to update the specific record
            int result = HP_UpdateRecord(chunk->file_desc, blockId, cursor, record);
            HP_Unpin(chunk->file_desc, blockId); // added this
            return result;
        }

        // Update the totalRecords counter for the next block
        totalRecords += recordsInBlock;
    }

    // If the function reaches here, the ith record is not found within the chunk
    return -1; // Or handle the case as appropriate for your application
}

void CHUNK_Print(CHUNK chunk)
{
    printf("Chunk from Block %d to Block %d:\n\n", chunk.from_BlockId, chunk.to_BlockId);

    Record record;
    // Iterate through each record in the chunk and print it

    for (int i = 0; i < chunk.recordsInChunk; i++)
    {
        // Use CHUNK_GetIthRecordInChunk to retrieve the ith record

        if (CHUNK_GetIthRecordInChunk(&chunk, i, &record) == 0)
        {
            // Print the record details 

            printRecord(record);
        }
        else
        {
            // Handle the case when record retrieval fails

            printf("Unable to retrieve record %d\n", i);
        }
    }
}

CHUNK_RecordIterator CHUNK_CreateRecordIterator(CHUNK *chunk)
{
    CHUNK_RecordIterator iterator;
    iterator.chunk = *chunk;
    iterator.currentBlockId = chunk->from_BlockId;
    iterator.cursor = 0; // Start from the first record in the first block
    return iterator;
}

int CHUNK_GetNextRecord(CHUNK_RecordIterator *iterator, Record *record)
{
    if (iterator->currentBlockId > HP_GetIdOfLastBlock(iterator->chunk.file_desc))
    {
        return -1;
    }
    if (iterator->currentBlockId <= iterator->chunk.from_BlockId + iterator->chunk.blocksInChunk)
    {
        int result = HP_GetRecord(iterator->chunk.file_desc, iterator->currentBlockId, iterator->cursor, record);
        HP_Unpin(iterator->chunk.file_desc, iterator->currentBlockId);
        if (result == 0)
        {
            // Move to the next record
            iterator->cursor++;
            if (iterator->cursor >= HP_GetRecordCounter(iterator->chunk.file_desc, iterator->currentBlockId))
            {
                iterator->cursor = 0;
                iterator->currentBlockId++;
                if (iterator->currentBlockId >= iterator->chunk.from_BlockId + iterator->chunk.blocksInChunk)
                {
                    return -1;
                }
            }
            return 0; // Success
        }
    }
    return -1; // No more records
}