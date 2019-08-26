#include <string.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include "BF.h"
#include "HT.h"

#define MAX_RECSPERBLOCK 2


/*struct HT_info {
	int fileDesc;
	char attrType;
	char* attrName;
	int attrLength;
	long int numBuckets;
};
*/

int primarydesc=0;

int hashFunction(int n,int numOfBuckets)
{      
    return ((n%numOfBuckets) +1);
}

int HT_CreateIndex( char *fileName, char attrType,char* attrName, int attrLength, int buckets)
{
	void *block,*indexBlock,*bucketblock;
    char  typeOfFile[3]={'p','r','i'};
    int block_number,i,offset=0;

    if (BF_CreateFile(fileName) < 0) //                                                                                                                                                 CREATE FILE
    {
        BF_PrintError("Error creating file");
        exit(EXIT_FAILURE);
    }
    int fileDesc;
    if ((fileDesc = BF_OpenFile(fileName)) < 0) //                                                                                                                                OPEN CREATED FILE
    {
        BF_PrintError("Error opening file");
        exit(EXIT_FAILURE);
    }

    printf("\tHT_CreateIndex: Created file : '%s' with fileDesc=%d\n",fileName,fileDesc);

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
    // COPY TO BLOCK 0 : TYPE OF FILE (IN THIS CASE pri , AS OPPOSED TO sec), ATTRTYPE, ATTRNAME, ATTRLENGTH, NUMBER OF BUCKETS
    memcpy( block,typeOfFile,3) ;
    memcpy( block+3,&attrType,1) ;
    memcpy( block+4,attrName,20) ;
    memcpy( block+24,&attrLength,sizeof(int)) ;
    memcpy( block+24+sizeof(int),&buckets,sizeof(int)) ;

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

        memcpy(indexBlock+offset,&block_number,sizeof(int));//COPY THE NUMBER OF EACH OF THE BUCKET BLOCKS TO THE INDEX BLOCK (BLOCK 1)                                                                                            
        offset+=sizeof(int);
    }

    if (BF_WriteBlock(fileDesc,1) < 0 )//                                                                                                                                             WRITE BLOCK 1
    {
        BF_PrintError("Error writing block");
        return -1; 
    }
    BF_CloseFile(fileDesc);

    primarydesc=fileDesc;
    //printf("\n\nprimarydesc is %d\n\n",primarydesc );

    printf("\tHT_CreateIndex: Made file's first, 'special' block. It contains: typeOfFile=%s, attrType=%c, attrName=%c, attrLength=%d, buckets=%ld\n",typeOfFile,attrType,attrName,attrLength,buckets);
    printf("\tHT_CreateIndex: Also, made the second block an 'index' block and blocks 3 to %d the first block for buckets 1 to %d respectively. All done.\n",buckets+2,buckets);
    return 0;
}

HT_info* HT_OpenIndex( char *fileName)
{
    void * block;
    HT_info *rettuple;

    int fileDesc = BF_OpenFile(fileName); 
    if (fileDesc<0)
    {
        BF_PrintError("Error opening file");
        return NULL; 
    }

    BF_ReadBlock(fileDesc,0,&block);

    rettuple=(HT_info *) malloc(sizeof(HT_info));//                                                                                          CREATE HT_info ITEM AND FILL IT WITH INFO FROM BLOCK 0

    rettuple->fileDesc=fileDesc;
    memcpy(&(rettuple->attrType),block+ 3,sizeof(char)); 
    memcpy(rettuple->attrName,block+ 4,20);
    memcpy(&(rettuple->attrLength),block+24,sizeof(int));
    memcpy(&(rettuple->numBuckets),block+24+sizeof(int),sizeof(long int));

    printf("\tHT_OpenIndex: Returning Header info for file: attrType=%c, attrName=%s, attrLength=%d, numBuckets=%ld\n",rettuple->attrType,rettuple->attrName,rettuple->attrLength,rettuple->numBuckets );
    return rettuple;
}

int HT_CloseIndex( HT_info* header_info )
{ 
    if ( BF_CloseFile(header_info->fileDesc) != 0)
    {   
        BF_PrintError("Error closing file");
        return -1;
    }
    else
    {
        free(header_info);
        printf("\tHT_CloseIndex: Index closing. All done\n");
        return 0;
    }
}

int insert_to_bucket(HT_info header_info, Record record, int bucket)
{
    void*block,*newblock;
    Record r;
    int fileDesc=header_info.fileDesc;
    BF_ReadBlock(fileDesc,bucket+1,&block);//                                                                                                                           READ FIRST BLOCK OF BUCKET
    int num_of_records,next_block,this_block;
    memcpy(&num_of_records,block,sizeof(int));
    memcpy(&next_block,block+sizeof(int),sizeof(int));
    if (num_of_records<MAX_RECSPERBLOCK)//                                                                                                               IF IT'S NOT FULL, INSERT THE RECORD THERE
    {
        printf("\tHT_InsertEntry: Record %d hashes to bucket %d. It is inserted into its first block.",record.id,bucket);
        memcpy(block+2*sizeof(int)+num_of_records*sizeof(Record),&record,sizeof(Record));
        num_of_records++;
        memcpy(block,&num_of_records,sizeof(int));

        BF_WriteBlock(fileDesc,bucket+1);

        printf(" The block now contains : {%d,%d",num_of_records,next_block);
        for (int i = 0; i < num_of_records; ++i)
        {
            memcpy(&r,block+2*sizeof(int)+i*sizeof(Record),sizeof(Record));
            printf(",%s",r.name );
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
        return (bucket+2);
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
        printf("\tHT_InsertEntry: Record %d hashes to bucket %d .Writing into the available connected block: %d.",record.id,bucket,this_block );
        memcpy(block+2*sizeof(int)+num_of_records*sizeof(Record),&record,sizeof(Record));
        num_of_records++;
        memcpy(block,&num_of_records,sizeof(int));

        BF_WriteBlock(fileDesc,this_block);

        printf("The block now contains : {%d,%d",num_of_records,next_block);
        for (int i = 0; i < num_of_records; ++i)
        {
            memcpy(&r,block+2*sizeof(int)+i*sizeof(Record),sizeof(Record));
            printf(",%s",r.name );
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
        return this_block;

    }
}

int HT_InsertEntry( HT_info header_info, Record record)
{
    int hasharg=0;
    if (header_info.attrType=='i')//                                                                                                                                FIND HASH VALUE FOR GIVEN VALUE
    {
        hasharg = record.id;
    }
    else if (header_info.attrType=='c')
    {
        if (strcmp(header_info.attrName, "name")==0)
        {
            char clone[15];
            strcpy(clone,record.name);
            for (int i = 0; i < strlen(record.name); ++i)
            {
                hasharg=hasharg+(int)clone[i];
            }
            //hasharg = atoi(clone);
            //printf("attr name=%s : hasharg=%d\n",clone,hasharg );
        }
        else if (strcmp(header_info.attrName, "surname")==0)
        {
            char clone[20];
            strcpy(clone,record.surname);
            for (int i = 0; i < strlen(record.surname); ++i)
            {
                hasharg=hasharg+(int)clone[i];
            }
            //hasharg = atoi(clone);
            //printf("attr surname=%s : hasharg=%d\n",clone,hasharg );
        }
        else if (strcmp(header_info.attrName, "address")==0)
        {
            char clone[25];
            strcpy(clone,record.address);
            for (int i = 0; i < strlen(record.address); ++i)
            {
                hasharg=hasharg+(int)clone[i];
            }
            //hasharg = atoi(clone);
            //printf("attr address=%s : hasharg=%d\n",clone,hasharg );
        }
    }

    int bucket=hashFunction(hasharg,header_info.numBuckets);        //                                                                                                             HASH VALUE FOUND
    return insert_to_bucket(header_info,record,bucket);
}

int HT_DeleteEntry( HT_info header_info, void *value )
{
    int block_num, entries, i,hasharg,found=0,read_blocks=0,next=0;
    void* block;
    Record* record;
    Record emptyrec;
    record = malloc(sizeof(Record));
    emptyrec.id=-1;
    sprintf(emptyrec.name,NULL);
    sprintf(emptyrec.surname,NULL);
    sprintf(emptyrec.address,NULL);

    char a[10];

    if (header_info.attrType=='i')//                                                                                                                                FIND HASH VALUE FOR GIVEN VALUE
    {
        sprintf(a,"%d",*(int*)value);
        hasharg = atoi(a); 
        printf("\tHT_DeleteEntry: Will delete record with id = %d\n",hasharg);
    }
    else if (header_info.attrType=='c')
    {
        if (strcmp(header_info.attrName, "name")==0)
        {
            char clone[15];
            strcpy(clone,(char*)value);
            for (int i = 0; i < strlen((char*)value); ++i)
            {
                hasharg=hasharg+(int)clone[i];
            }
        }
        else if (strcmp(header_info.attrName, "surname")==0)
        {
            char clone[20];
            strcpy(clone,(char*)value);
            for (int i = 0; i < strlen((char*)value); ++i)
            {
                hasharg=hasharg+(int)clone[i];
            }
        }
        else if (strcmp(header_info.attrName, "address")==0)
        {
            char clone[25];
            strcpy(clone,(char*)value);
            for (int i = 0; i < strlen((char*)value); ++i)
            {
                hasharg=hasharg+(int)clone[i];
            }
        }
    }

    block_num = hashFunction( hasharg , header_info.numBuckets) + 1;//                                                                                                             HASH VALUE FOUND


    while(next>=0) //NEXT>=0 IF THE BUCKET "POINTS" TO AT LEAST ONE MORE BLOCK , SO DO THE FOLLOWING WHILE THERE ARE MORE BLOCKS TO BE TRAVERSED
    {

        BF_ReadBlock(header_info.fileDesc, block_num, &block);
        read_blocks++;
        memcpy(&entries, block, sizeof(int));           //first int: number of records
        memcpy(&next, block+sizeof(int), sizeof(int));  //second int: next block for given bucket (if it exists)

        for(i = 0; i < entries; i++)                                                                                                                              // FOR EVERY BLOCK IN THE RECORD
        {
            memcpy(record, block+2*sizeof(int)+i*sizeof(Record), sizeof(Record));
                        // AFTER FINDING THE TYPE OF 'VALUE', COMPARE IT TO THE RECORD'S CORRESPONDING ATTRIBUTE . IF THEY ARE THE SAME , WE HAVE A MATCH. DELETE IT (following if-else if do that)
            if(header_info.attrType == 'i')
            {
                if(record->id == *(int *)value)
                {
                    printf("\tHT_DeleteEntry: Record found : %d %s %s %s, will now delete it\n", record->id, record->name, record->surname, record->address);
                    found=1;
                    memcpy(block+2*sizeof(int)+i*sizeof(Record),&emptyrec,sizeof(Record));
                    BF_WriteBlock(header_info.fileDesc,block_num);
                    return 0;
                }
                    
            }      
            else if(header_info.attrType == 'c')
            {
                if(!strcmp(header_info.attrName,"name"))
                {
                    if(!strcmp(record->name, value))
                    {   
                        printf("\tHT_DeleteEntry: Record found : %d %s %s %s, will now delete it\n", record->id, record->name, record->surname, record->address);
                        found=1;
                        memcpy(block+2*sizeof(int)+i*sizeof(Record),&emptyrec,sizeof(Record));
                        BF_WriteBlock(header_info.fileDesc,block_num);
                        return 0;
                    }
                }
                else if(!strcmp(header_info.attrName, "surname"))      
                {
                    if(!strcmp(record->surname, value))
                    {
                        printf("\tHT_DeleteEntry: Record found : %d %s %s %s, will now delete it\n", record->id, record->name, record->surname, record->address);
                        found=1;
                        memcpy(block+2*sizeof(int)+i*sizeof(Record),&emptyrec,sizeof(Record));
                        BF_WriteBlock(header_info.fileDesc,block_num);
                        return 0;
                    }
                }
                else if(!strcmp(header_info.attrName,"address"))
                {
                    if(!strcmp(record->address, value))
                    {
                        printf("\tHT_DeleteEntry: Record found : %d %s %s %s, will now delete it\n", record->id, record->name, record->surname, record->address);
                        found=1;
                        memcpy(block+2*sizeof(int)+i*sizeof(Record),&emptyrec,sizeof(Record));
                        BF_WriteBlock(header_info.fileDesc,block_num);
                        return 0;
                    }
                }
            }
        }
        block_num = next;
    }

    return -1;
}

int HT_GetAllEntries(HT_info header_info, void *value)
{

    int block_num, entries, i,hasharg=0,found=0,read_blocks=0;
    void* block;
    Record* record;
    record = malloc(sizeof(Record));
    char a[10];

    if (header_info.attrType=='i')//                                                                                                                              FIND HASH VALUE FOR GIVEN VALUE 
    {

        sprintf(a,"%d",*(int*)value);
        hasharg = atoi(a);
    }
    else if (header_info.attrType=='c')
    {
        if (strcmp(header_info.attrName, "name")==0)
        {
            char clone[15];
            strcpy(clone,(char*)value);
            for (int i = 0; i < strlen((char*)value); ++i)
            {
                hasharg=hasharg+(int)clone[i];
            }
            //printf("{%s},%d\n",(char*)value,hasharg );
        }
        else if (strcmp(header_info.attrName, "surname")==0)
        {
            char clone[20];
            strcpy(clone,(char*)value);
            for (int i = 0; i < strlen((char*)value); ++i)
            {
                hasharg=hasharg+(int)clone[i];
            }
        }
        else if (strcmp(header_info.attrName, "address")==0)
        {
            char clone[25];
            strcpy(clone,(char*)value);
            for (int i = 0; i < strlen((char*)value); ++i)
            {
                hasharg=hasharg+(int)clone[i];
            }
        }
    }

    block_num = hashFunction( hasharg , header_info.numBuckets) + 1;//                                                                                                           HASH VALUE FOUND
    int next=0;

    printf("\tHT_GetAllEntries: ");
    while(next>=0)//NEXT>=0 IF THE BUCKET "POINTS" TO AT LEAST ONE MORE BLOCK , SO DO THE FOLLOWING WHILE THERE ARE MORE BLOCKS TO BE TRAVERSED
    {

        BF_ReadBlock(header_info.fileDesc, block_num, &block);
        read_blocks++;

        memcpy(&entries, block, sizeof(int));//first int: number of records
        memcpy(&next, block+sizeof(int), sizeof(int));//second int: next block for given bucket (if it exists)

        for(i = 0; i < entries; i++)//                                                                                                                               FOR EVERY BLOCK IN THE RECORD
        {
            memcpy(record, block+2*sizeof(int)+i*sizeof(Record), sizeof(Record));
                        // AFTER FINDING THE TYPE OF 'VALUE', COMPARE IT TO THE RECORD'S CORRESPONDING ATTRIBUTE . IF THEY ARE THE SAME , WE HAVE A MATCH. PRINT IT (following if-else if do that)
            if(header_info.attrType == 'i')
            {
                if(record->id == *(int *)value)
                {
                    if (found==0)
                    {
                        printf("Record(s) found:  ");
                    }
                    printf("%d %s %s %s,   ", record->id, record->name, record->surname, record->address);
                    found=1;
                }
                    
            }      
            else if(header_info.attrType == 'c')
            {
                if(!strcmp(header_info.attrName,"name"))
                {
                    if(!strcmp(record->name, value))
                    {
                        if (found==0)
                        {
                            printf("Record(s) found:  ");
                        }
                        printf("%d %s %s %s,   ", record->id, record->name, record->surname, record->address);
                        found=1;
                    }
                }
                else if(!strcmp(header_info.attrName, "surname"))      
                {
                    if(!strcmp(record->surname, value))
                    {
                        if (found==0)
                        {
                            printf("Record(s) found:  ");
                        }
                        printf("%d %s %s %s,   ", record->id, record->name, record->surname, record->address);
                        found=1;
                    }

                }
                else if(!strcmp(header_info.attrName, "address"))
                {
                    if(!strcmp(record->address, value))
                    {
                        if (found==0)
                        {
                            printf("Record(s) found:  ");
                        }
                        printf("%d %s %s %s,   ", record->id, record->name, record->surname, record->address);
                        found=1;
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