#include <string.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include "BF.h"
#include "HT.h"
#include "SHT.h"

#define MAX_RECSPERBLOCK 2



/*typedef struct SecondaryRecord {
	int blockId;
	Record record;
} SecondaryRecord;
*/


int SHT_CreateSecondaryIndex( char *sfileName,char* attrName, int attrLength, int buckets , char* fileName)
{
    void *block,*indexBlock,*bucketblock;
    char  typeOfFile[3]={'s','e','c'};
    int block_number,i,offset=0;

    if (BF_CreateFile(sfileName) < 0) //                                                                                                                                                 CREATE FILE
    {
        BF_PrintError("Error creating file");
        exit(EXIT_FAILURE);
    }
    int fileDesc;
    if ((fileDesc = BF_OpenFile(sfileName)) < 0) //                                                                                                                                OPEN CREATED FILE
    {
        BF_PrintError("Error opening file");
        exit(EXIT_FAILURE);
    }

    printf("\tSHT_CreateIndex: Created file : '%s' with fileDesc=%d\n",sfileName,fileDesc);

    if (BF_AllocateBlock(fileDesc) < 0) 
    {
        BF_PrintError("Error allocating block - same old");
        exit(EXIT_FAILURE);
    }
  
    if (BF_ReadBlock(fileDesc, 0, &block) < 0 )//                                                                                                                         ALLOCATE AND READ BLOCK 0
    {
        BF_PrintError("Error reading block");
        return -1;
    }
                                                                        // COPY TO BLOCK 0 : TYPE OF FILE (IN THIS CASE sec , AS OPPOSED TO pri), ATTRTYPE, ATTRNAME, ATTRLENGTH, NUMBER OF BUCKETS
    memcpy( block,typeOfFile,3) ;
    memcpy( block+3,attrName,20) ;
    memcpy( block+23,&attrLength,sizeof(int)) ;
    memcpy( block+23+sizeof(int),&buckets,sizeof(int)) ;
    memcpy( block+23+2*sizeof(int),fileName,20) ;

    if (BF_WriteBlock(fileDesc, 0) < 0 ) //                                                                                                                                           WRITE BLOCK 0
    {
        BF_PrintError("Error writing block");
        return -1; 
    }

    if (BF_AllocateBlock(fileDesc) < 0 )
    {
        BF_PrintError("Error allocating block");
        return -1; 
    }
    if (BF_ReadBlock(fileDesc, 1, &indexBlock) < 0 ) //                                                                                                                   ALLOCATE AND READ BLOCK 1
    {
        BF_PrintError("Error reading block");
        return -1; 
    }

    for (int i = 1; i<=buckets; i++) 
    {
        if (BF_AllocateBlock(fileDesc) < 0 )//                                                                                                                    ALLOCATE ONE BLOCK FOR EACH BUCKET
        {
            BF_PrintError("Error allocating block");
            return -1;
        }
                       //BUCKET BLOCKS: COPY CURRENT NUMBER OF RECORDS (O FOR NOW) OF THIS BLOCK AND THE 'LINK' TO THE NEXT BLOCK OF THE BUCKET (-1,MEANING NO LINK EXISTS FOR NOW) INSIDE THE BLOCK
        BF_ReadBlock(fileDesc,i+1,&bucketblock);
        int num_of_records=0,next_block=-1;
        memcpy(bucketblock,&num_of_records,sizeof(int));
        memcpy(bucketblock+sizeof(int),&next_block,sizeof(int));
        BF_WriteBlock(fileDesc,i+1);

        block_number=BF_GetBlockCounter(fileDesc)-1;

        memcpy(indexBlock+offset,&block_number,sizeof(int));//                                                             COPY THE NUMBER OF EACH OF THE BUCKET BLOCKS TO THE INDEX BLOCK (BLOCK 1)                                                                                            
        offset+=sizeof(int);
    }

    if (BF_WriteBlock(fileDesc,1) < 0 )//                                                                                                                                             WRITE BLOCK 1
    {
        BF_PrintError("Error writing block");
        return -1; 
    }
    BF_CloseFile(fileDesc);

    printf("\tSHT_CreateIndex: Made file's first, 'special' block. It contains: typeOfFile=%s, attrName=%c, attrLength=%d, buckets=%ld\n",typeOfFile,attrName,attrLength,buckets);
    printf("\tSHT_CreateIndex: Also, made the second block an 'index' block and blocks 3 to %d the first block for buckets 1 to %d respectively. All done.\n",buckets+2,buckets);

    int blockId=1,nor=0,nxt=-1;
    void *tmpblock;
    Record vessel;
    for (int i = 2; i < BF_GetBlockCounter(primarydesc); i++)
    {
        BF_ReadBlock(primarydesc , i , &block);
        memcpy(&nor,block,sizeof(int));
        memcpy(&nxt,block+sizeof(int),sizeof(int));
        Record rectable[nor];
        for (int j = 0; j < nor; j++)
        {  
            memcpy(&rectable[j],block+2*sizeof(int)+j*sizeof(Record),sizeof(Record));
        }
        for (int j = 0; j < nor; j++)
        {
            SecondaryRecord sRecord;
            sRecord.record=rectable[j];
            sRecord.blockId=i;
            SHT_info* shi=SHT_OpenSecondaryIndex(sfileName);
            int sInsertError=SHT_SecondaryInsertEntry(*shi,sRecord);
            int shtCloseError=SHT_CloseSecondaryIndex(shi);
        }
    }
    printf("\tSHT_CreateIndex: Finally, all records that existed on the primary index are now inserted to this secondary index too. All done.\n");
    return 0;

}

SHT_info* SHT_OpenSecondaryIndex( char *sfileName)
{
    void * block;
    SHT_info *rettuple;
    int fileDesc = BF_OpenFile(sfileName); 
    if (fileDesc<0)
    {
        BF_PrintError("Error opening file");
        return NULL; 
    }

    BF_ReadBlock(fileDesc,0,&block);

    rettuple=(SHT_info *) malloc(sizeof(SHT_info));

    rettuple->fileDesc=fileDesc;
    memcpy(rettuple->attrName,block+ 3,20);
    memcpy(&(rettuple->attrLength),block+23,sizeof(int));
    memcpy(&(rettuple->numBuckets),block+23+sizeof(int),sizeof(int));
    memcpy(&(rettuple->fileName),block+23+2*sizeof(int),20);

    printf("\tSHT_OpenIndex: Returning Header info for file: attrName=%s, attrLength=%d, numBuckets=%ld, fileName=%s\n",rettuple->attrName,rettuple->attrLength,rettuple->numBuckets,rettuple->fileName );

    return rettuple;
}

int SHT_CloseSecondaryIndex( SHT_info* header_info ){

    if ( BF_CloseFile(header_info->fileDesc) != 0)
    {   
        BF_PrintError("Error closing file");
        return -1;
    }
    else
    {
        free(header_info);
        printf("\tSHT_CloseIndex: Index closing. All done\n");
        return 0;
    }
}

int s_insert_to_bucket(SHT_info header_info, SecondaryRecord record, int bucket)
{
    void*block,*newblock;
    SecondaryRecord r;
    int fileDesc=header_info.fileDesc;
    BF_ReadBlock(fileDesc,bucket+1,&block);//                                                                                                                           READ FIRST BLOCK OF BUCKET
    int num_of_records,next_block,this_block;
    memcpy(&num_of_records,block,sizeof(int));
    memcpy(&next_block,block+sizeof(int),sizeof(int));
    if (num_of_records<MAX_RECSPERBLOCK)//                                                                                                               IF IT'S NOT FULL, INSERT THE RECORD THERE
    {
        printf("\tSHT_InsertEntry: Record %d hashes to bucket %d. It is inserted into its first block.",record.record.id,bucket);
        memcpy(block+2*sizeof(int)+num_of_records*sizeof(SecondaryRecord),&record,sizeof(SecondaryRecord));
        num_of_records++;
        memcpy(block,&num_of_records,sizeof(int));

        BF_WriteBlock(fileDesc,bucket+1);

        printf(" The block now contains : {%d,%d",num_of_records,next_block);
        for (int i = 0; i < num_of_records; ++i)
        {
            memcpy(&r,block+2*sizeof(int)+i*sizeof(SecondaryRecord),sizeof(SecondaryRecord));
            printf(",%s",r.record.name );
        }
        if (num_of_records==MAX_RECSPERBLOCK)//                               IF THE BLOCK IS NOW FULL, ALLOCATE A NEW ONE AND PUT THE NEW ONE'S NUMBER IN THIS ONE'S 'NEXT' FIELD (THE SECOND INT)
        {
            
            int c = BF_GetBlockCounter(fileDesc);
            memcpy(block+sizeof(int),&c,sizeof(int));
            printf("}(linking bucket %d to block %d - block %d full)\n",bucket,c,bucket+2);
            BF_WriteBlock(fileDesc,bucket+1);

            BF_AllocateBlock(fileDesc);
            BF_ReadBlock(fileDesc,c,&newblock);
            int nor=0,nb=-1;
            memcpy(newblock,&nor,sizeof(int));
            memcpy(newblock+sizeof(int),&nb,sizeof(int));
            BF_WriteBlock(fileDesc,c);
        }
        else
        {
            printf("}\n");
        }
        return 0;
    }
    else//                                                                                                                                                          IF FIRST BLOCK OF BUCKET IS FULL
    {
        while (num_of_records>=MAX_RECSPERBLOCK)//                                                      TRAVERSE THE BLOCK'S NEXT , AND THEN THE NEXT'S NEXT ETC UNTILL YOU FIND ONE THAT'S NOT FULL
        {
            this_block=next_block;
            BF_ReadBlock(fileDesc,next_block,&block);
            memcpy(&num_of_records,block,sizeof(int));
            memcpy(&next_block,block+sizeof(int),sizeof(int));
        }
        printf("\tSHT_InsertEntry: Record %d hashes to bucket %d .Writing into the available connected block: %d.",record.record.id,bucket,this_block );
        memcpy(block+2*sizeof(int)+num_of_records*sizeof(SecondaryRecord),&record,sizeof(SecondaryRecord));
        num_of_records++;
        memcpy(block,&num_of_records,sizeof(int));

        BF_WriteBlock(fileDesc,this_block);

        printf("The block now contains : {%d,%d",num_of_records,next_block);
        for (int i = 0; i < num_of_records; ++i)
        {
            memcpy(&r,block+2*sizeof(int)+i*sizeof(SecondaryRecord),sizeof(SecondaryRecord));
            printf(",%s",r.record.name );
        }
        if (num_of_records==MAX_RECSPERBLOCK)//                               IF THE BLOCK IS NOW FULL, ALLOCATE A NEW ONE AND PUT THE NEW ONE'S NUMBER IN THIS ONE'S 'NEXT' FIELD (THE SECOND INT)
        {
            int c = BF_GetBlockCounter(fileDesc);
            memcpy(block+sizeof(int),&c,sizeof(int));
            printf("} (linking bucket %d to block %d - block %d full )\n",bucket,c,this_block);
            BF_WriteBlock(fileDesc,bucket+1);

            BF_AllocateBlock(fileDesc);
            BF_ReadBlock(fileDesc,c,&newblock);
            int nor=0,nb=-1;
            memcpy(newblock,&nor,sizeof(int));
            memcpy(newblock+sizeof(int),&nb,sizeof(int));
            BF_WriteBlock(fileDesc,c);
        }
        else
        {
            printf("}\n");
        }

    }
}

int SHT_SecondaryInsertEntry( SHT_info header_info,SecondaryRecord record)
{
    int hasharg=0;
    if (strcmp(header_info.attrName, "name")==0)
    {
        char clone[15];
        strcpy(clone,record.record.name);
        for (int i = 0; i < strlen(record.record.name); ++i)
        {
            hasharg=hasharg+(int)clone[i];
        }
        //hasharg = atoi(clone);
        //printf("attr name=%s : hasharg=%d\n",clone,hasharg );
    }
    else if (strcmp(header_info.attrName, "surname")==0)
    {
        char clone[20];
        strcpy(clone,record.record.surname);
        for (int i = 0; i < strlen(record.record.surname); ++i)
        {
            hasharg=hasharg+(int)clone[i];
        }
        //hasharg = atoi(clone);
        //printf("attr surname=%s : hasharg=%d\n",clone,hasharg );
    }
    else if (strcmp(header_info.attrName, "address")==0)
    {
        char clone[25];
        strcpy(clone,record.record.address);
        for (int i = 0; i < strlen(record.record.address); ++i)
        {
            hasharg=hasharg+(int)clone[i];
        }
        //hasharg = atoi(clone);
        //printf("attr address=%s : hasharg=%d\n",clone,hasharg );
    }
    

    int bucket=hashFunction(hasharg,header_info.numBuckets);        //                                                                                                             HASH VALUE FOUND
    return s_insert_to_bucket(header_info,record,bucket);
}

int SHT_SecondaryGetAllEntries(SHT_info header_info_sht,HT_info header_info_ht,void *value)
{
    int block_num, entries, i,hasharg=0,found=0,read_blocks=0;
    void* block, *pblock;
    SecondaryRecord* record;
    record = malloc(sizeof(SecondaryRecord));
    char a[10];


    if (strcmp(header_info_sht.attrName, "name")==0)
    {
        char clone[15];
        strcpy(clone,(char*)value);
        for (int i = 0; i < strlen((char*)value); ++i)
        {
            hasharg=hasharg+(int)clone[i];
        }
        //printf("{%s},%d\n",(char*)value,hasharg );
    }
    else if (strcmp(header_info_sht.attrName, "surname")==0)
    {
        char clone[20];
        strcpy(clone,(char*)value);
        for (int i = 0; i < strlen((char*)value); ++i)
        {
            hasharg=hasharg+(int)clone[i];
        }
    }
    else if (strcmp(header_info_sht.attrName, "address")==0)
    {
        char clone[25];
        strcpy(clone,(char*)value);
        for (int i = 0; i < strlen((char*)value); ++i)
        {
            hasharg=hasharg+(int)clone[i];
        }
    }

    block_num = hashFunction( hasharg , header_info_sht.numBuckets) + 1;//                                                                                                           HASH VALUE FOUND
    int next=0;

    printf("\tSHT_GetAllEntries: ");
    while(next>=0)//                                                  NEXT>=0 IF THE BUCKET "POINTS" TO AT LEAST ONE MORE BLOCK , SO DO THE FOLLOWING WHILE THERE ARE MORE BLOCKS TO BE TRAVERSED
    {

        BF_ReadBlock(header_info_sht.fileDesc, block_num, &block);
        read_blocks++;

        memcpy(&entries, block, sizeof(int));//first int: number of records
        memcpy(&next, block+sizeof(int), sizeof(int));//second int: next block for given bucket (if it exists)

        for(i = 0; i < entries; i++)//                                                                                                                               FOR EVERY BLOCK IN THE RECORD
        {
            memcpy(record, block+2*sizeof(int)+i*sizeof(SecondaryRecord), sizeof(SecondaryRecord));
                        // AFTER FINDING THE TYPE OF 'VALUE', COMPARE IT TO THE RECORD'S CORRESPONDING ATTRIBUTE . IF THEY ARE THE SAME , WE HAVE A MATCH. PRINT IT (following if-else if do that)    
        
            if(!strcmp(header_info_sht.attrName,"name"))
            {
                if(!strcmp(record->record.name, value))
                {
                    
                    printf("will look for '%s' in primary's block %d ... ",record->record.name, record->blockId);
                    BF_ReadBlock(header_info_ht.fileDesc,record->blockId,&pblock);
                    int nor;
                    memcpy(&nor, pblock, sizeof(int));
                    Record* precord;
                    precord = malloc(sizeof(Record));
                    for (int j = 0; j <= nor; j++)
                    {
                        memcpy(precord, block+2*sizeof(int)+j*sizeof(Record), sizeof(Record));
                        if (strcmp(precord->name,record->record.name))
                        {
                            printf("Record found:  %d %s %s %s\n",record->record.id,record->record.name,record->record.surname,record->record.address );
                            return read_blocks;
                        }
                    }
                }
            }
            else if(!strcmp(header_info_sht.attrName, "surname"))      
            {
                if(!strcmp(record->record.surname, value))
                {
                    printf("will look for '%s' in primary's block %d ... ",record->record.surname, record->blockId);
                    BF_ReadBlock(header_info_ht.fileDesc,record->blockId,&pblock);
                    int nor;
                    memcpy(&nor, pblock, sizeof(int));
                    Record* precord;
                    precord = malloc(sizeof(Record));
                    for (int j = 0; j <= nor; j++)
                    {
                        memcpy(precord, block+2*sizeof(int)+j*sizeof(Record), sizeof(Record));
                        if (strcmp(precord->surname,record->record.surname))
                        {
                            printf("Record found:  %d %s %s %s\n",record->record.id,record->record.name,record->record.surname,record->record.address );
                            return read_blocks;
                        }
                    }
                }

            }
            else if(!strcmp(header_info_sht.attrName, "address"))
            {
                if(!strcmp(record->record.address, value))
                {
                    printf("will look for '%s' in primary's block %d ... ",record->record.address, record->blockId);
                    BF_ReadBlock(header_info_ht.fileDesc,record->blockId,&pblock);
                    int nor;
                    memcpy(&nor, pblock, sizeof(int));
                    Record* precord;
                    precord = malloc(sizeof(Record));
                    for (int j = 0; j <= nor; j++)
                    {
                        memcpy(precord, block+2*sizeof(int)+j*sizeof(Record), sizeof(Record));
                        if (strcmp(precord->address,record->record.address))
                        {
                            printf("Record found:  %d %s %s %s\n",record->record.id,record->record.name,record->record.surname,record->record.address );
                            return read_blocks;
                        }
                    }
                }
            }
        
        }
        block_num = next;
    }
    printf("(%d Blocks read in total",read_blocks);
    if (found==0)
    {
        printf("- no records with the given value found)\n");
        return -1;
    }
    printf(")\n");
    return read_blocks;
}	