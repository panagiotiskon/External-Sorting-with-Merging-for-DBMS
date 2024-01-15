#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf.h"
#include "hp_file.h"
#include "record.h"
#include "sort.h"
#include "merge.h"
#include "chunk.h"

#include <stdbool.h>
#include <string.h>

void sortSingleChunk(CHUNK *chunk)
{
    int i, j, k;
    int numBlocks = chunk->to_BlockId - chunk->from_BlockId + 1;
    int maxRecords = HP_GetMaxRecordsInBlock(chunk->file_desc);

    // Allocate temporary memory to store all records in the chunk
    Record *allRecords = (Record *)malloc(sizeof(Record) * numBlocks * maxRecords);

    if (allRecords == NULL)
    {
        // Handle memory allocation error
        return;
    }

    // Copy all records from the chunk to the temporary array
    int recordIndex = 0;
    int recordsInBlock;
    for (i = 0; i < numBlocks; i++)
    {
        recordsInBlock = HP_GetRecordCounter(chunk->file_desc, chunk->from_BlockId + i);

        for (j = 0; j < maxRecords && j < recordsInBlock; j++)
        {
            if (CHUNK_GetIthRecordInChunk(chunk, i * maxRecords + j, &allRecords[recordIndex]) != 0)
            {
                // Handle the error or return as appropriate for your application
                free(allRecords);
                return;
            }
            recordIndex++;
        }
    }

    // Sort all records in the temporary array
    for (i = 0; i < recordIndex - 1; i++)
    {
        for (j = i + 1; j < recordIndex; j++)
        {
            if (shouldSwap(&allRecords[j], &allRecords[i]))
            {
                // Swap the records
                Record tempRecord = allRecords[i];
                allRecords[i] = allRecords[j];
                allRecords[j] = tempRecord;
            }
        }
    }

    // Copy sorted records back to the chunk
    recordIndex = 0;
    for (i = 0; i < numBlocks; i++)
    {
        int maxRecordsInBlock = (i == numBlocks - 1) ? recordsInBlock : maxRecords;

        for (j = 0; j < maxRecordsInBlock; j++)
        {
            CHUNK_UpdateIthRecord(chunk, i * maxRecords + j, allRecords[recordIndex]);
            recordIndex++;
        }
    }

    // Free temporary memory
    free(allRecords);
}
void selectionSortBlock(CHUNK *chunk)
{
    int i, j;
    int maxRecords = HP_GetMaxRecordsInBlock(chunk->file_desc);

    for (i = 0; i < maxRecords - 1; i++)
    {
        Record minRecord, tempRecord;
        int minIndex = i;

        // Find the minimum element in the unsorted part of the block
        if (CHUNK_GetIthRecordInChunk(chunk, i, &minRecord) != 0)
        {
            // Handle the error or return as appropriate for your application
            break;
        }

        for (j = i + 1; j < maxRecords; j++)
        {
            if (CHUNK_GetIthRecordInChunk(chunk, j, &tempRecord) == 0 &&
                shouldSwap(&tempRecord, &minRecord))
            {
                minRecord = tempRecord;
                minIndex = j;
            }
        }

        // Swap the found minimum element with the first element
        if (minIndex != i)
        {
            CHUNK_GetIthRecordInChunk(chunk, i, &tempRecord);
            CHUNK_UpdateIthRecord(chunk, i, minRecord);
            CHUNK_UpdateIthRecord(chunk, minIndex, tempRecord);
        }
    }
}

bool shouldSwap(Record *rec1, Record *rec2)
{
    // Compare names
    int nameComparison = strcmp(rec1->name, rec2->name);

    if (nameComparison == 0)
    {
        // Names are equal, compare surnames
        int surnameComparison = strcmp(rec1->surname, rec2->surname);

        if (surnameComparison == 0)
        {
            // Surnames are equal, compare cities
            return strcmp(rec1->city, rec2->city) < 0;
        }
        else
        {
            // Swap if rec1's surname is greater than rec2's surname
            return surnameComparison < 0;
        }
    }
    else
    {
        // Swap if rec1's name is greater than rec2's name
        return nameComparison < 0;
    }

    // Order is correct, no swap needed
    return false;
}

void sort_FileInChunks(int file_desc, int numBlocksInChunk)
{
    // Create a chunk iterator
    CHUNK_Iterator iterator = CHUNK_CreateIterator(file_desc, numBlocksInChunk);

    while (1)
    {
        CHUNK chunk;
        int result = CHUNK_GetNext(&iterator, &chunk);

        if (result != 0)
        {
            // No more chunks to retrieve
            break;
        }

        // Print unsorted chunk
        printf("--------------Unsorted Chunk:--------------------\n");
        CHUNK_Print(chunk);

        // Sort the chunk
        sort_Chunk(&chunk);

        // Print sorted chunk
        printf("---------------Sorted Chunk:----------------------\n");
        CHUNK_Print(chunk);
    }
    return;
}

// Function to sort each record in each block and blocks within the chunk
void sort_Chunk(CHUNK *chunk)
{
    // Sort records within each block
    int temp = chunk->from_BlockId;
    for (int i = chunk->from_BlockId; i <= chunk->to_BlockId; i++) // added this,iterate to every chunk block
    {
        chunk->from_BlockId = i;
        selectionSortBlock(chunk);
    }
    chunk->from_BlockId = temp; // added till here

    sortSingleChunk(chunk);

    return;
}

void printAllRecords(int file_desc)
{
    int lastBlockId = HP_GetIdOfLastBlock(file_desc);

    for (int blockId = 1; blockId <= lastBlockId; ++blockId)
    {
        printf("Records in Block %d:\n", blockId);

        Record record;
        int recordsInBlock = HP_GetRecordCounter(file_desc, blockId);

        for (int i = 0; i < recordsInBlock; ++i)
        {
            if (HP_GetRecord(file_desc, blockId, i, &record) == 0)
            {
                printRecord(record);
                HP_Unpin(file_desc, blockId);
            }
            else
            {
                printf("Unable to retrieve record %d in Block %d\n", i, blockId);
            }
        }
    }
}