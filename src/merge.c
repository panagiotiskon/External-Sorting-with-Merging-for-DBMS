#include <merge.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>

void merge(int inputFileDesc, int chunkSize, int bWay, int outputFileDesc)
{
    int i;

    // Create an iterator for chunks in the input file
    CHUNK_Iterator chunk_iter = CHUNK_CreateIterator(inputFileDesc, chunkSize);
    CHUNK chunks[bWay];

    // Initialize an array of chunks to be merged
    for (int i = 0; i < bWay; i++)
    {
        CHUNK_GetNext(&chunk_iter, &chunks[i]);
    }

    // Iterate through the records of each chunk
    CHUNK_RecordIterator recordIterators[bWay];
    for (int i = 0; i < bWay; i++)
    {
        recordIterators[i] = CHUNK_CreateRecordIterator(&chunks[i]);
    }

    Record records[bWay];
    int position[bWay];

    // Initialize position array to keep track of each chunk's current record
    for (int i = 0; i < bWay; i++)
    {
        position[i] = -1;
    }

    int flag_must_end = 0;
    int count = 0;

    while (1)
    {
        // Find the minimum record among the current records of each chunk
        for (int i = 0; i < bWay; i++)
        {
            if (position[i] == 0 || position[i] == -2)
            {
                continue;
            }
            if (CHUNK_GetNextRecord(&recordIterators[i], &records[i]) == 0)
            {
                position[i] = 0;
            }
            else
            {
                // No more records in the current chunk
                count++;
                position[i] = -2;

                // Check if only one chunk remains with records
                if (count == bWay - 1)
                {
                    for (int j = 0; j < bWay; j++)
                    {
                        if (position[j] != -2)
                        {
                            // Insert remaining records from the last chunk
                            Record temp;
                            while (CHUNK_GetNextRecord(&recordIterators[j], &temp) == 0)
                            {
                                HP_InsertEntry(outputFileDesc, temp);
                            }

                            // Set the flag to indicate the end of merging
                            flag_must_end = 1;
                            break;
                        }
                    }
                }
            }
        }

        // Check if we must end the merging process
        if (flag_must_end == 1)
        {
            break;
        }

               // Find the minimum record among the remaining records
        Record min_rec;
        for (int i = 0; i < bWay; i++)
        {
            if (position[i] != -2)
            {
                min_rec = records[i];
                break;
            }
        }
        int minChunkIndex = 0;
        for (int i = 1; i < bWay; i++)
        {
            if (position[i] != -2)
                if (shouldSwap(&records[i], &min_rec))
                {
                    min_rec = records[i];
                    minChunkIndex = i;
                }
        }

        // Update the position of the chunk with the minimum record
        if (position[minChunkIndex] != -2)
        {
            position[minChunkIndex] = 1;
        }

        // Insert the minimum record into the output file
        if (HP_InsertEntry(outputFileDesc, records[minChunkIndex]) == -1)
        {
            // Handle error (e.g., print an error message, close files, etc.)
            break;
        }
    }
}
