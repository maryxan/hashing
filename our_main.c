
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdlib.h>
#include "BF.h"
#include "HT.h"
#include "SHT.h"


int main(int argc,char* argv[])
{
	if (argc!=3)
	{
		printf("usage: ./run inputfile1.txt inputfile2.txt ( example : ./run records_named.txt records_named2.txt )\n");
		return 1;
	}
	char const* const inputfile = argv[1]; 
    FILE* file = fopen(inputfile, "r");
    char line[256];

	BF_Init();

	char* fileName="primary.index";
	char attrType='i';
	char* attrName="id";
	int attrLength=4;
	int buckets=5;
	int testRecordsNumber=10;
	char* sfileName="secondary.index";
	char sAttrType='c';
	char* sAttrName="name";
	int sAttrLength=15;
	int sBuckets=5;
	
	printf("Creating Index file\n");
	int createNumCode=HT_CreateIndex(fileName,attrType,attrName,attrLength,buckets);
	if (createNumCode!=0)
	{
		printf("Index creation failed\n");
		return 1;
	}
	HT_info* hi;

	printf("Opening the index\n");
	hi=HT_OpenIndex(fileName);
	if(hi==NULL || hi->attrType!=attrType || strcmp(hi->attrName,attrName)!=0)
	{
		printf("Index opening failed\n");
		return 1;
	}

	printf("Inserting Records from file %s(%d):\n",fileName,file);

	const char s[2] = ",";
   	char *token;
   	int counter=0,totalrecs=0;

	while (fgets(line, sizeof(line), file)) 
	{
		for (int i = 0; i < sizeof(line); ++i)
		{
			if (line[i]=='\n')
			{
				line[i]='\0';
			}
		}
        Record record;
       	token = strtok(line, s);
   		counter=0;

		while( token != NULL ) 
		{

	    	if (counter==0)
	    	{
	    		char *temp=malloc(strlen(token));
	    		int currid=-1;
	    		for (int i = 0; i < strlen(token)-1; ++i)
	    		{
	    			temp[i]=token[i+1];
	    		}
	    		currid=atoi(temp);
	    		//printf("[%d]-",currid );
	    		record.id=currid;
	    	}
	    	else if (counter==3)
	    	{
	    		char *temp=malloc(strlen(token)-2);
	    		for (int i = 0; i < strlen(token)-3; ++i)
	    		{
	    			temp[i]=token[i+1];
	    		}
	    		//printf("[%s]\n",temp );
	    		sprintf(record.address,"%s",temp);
	    	}
	    	else
	    	{
				char *temp=malloc(strlen(token)-1);
	    		for (int i = 0; i < strlen(token)-2; ++i)
	    		{
	    			temp[i]=token[i+1];
	    		}
	    		//printf("[%s]-",temp );
	    		if (counter==1)
	    		{
	    			sprintf(record.name,"%s",temp);
	    		}
	    		else
	    		{
	    			sprintf(record.surname,"%s",temp);	
	    		}
	    	}
	    	counter++;
	    	token = strtok(NULL, s);
		}
		int ret=HT_InsertEntry(*hi,record);
		totalrecs++;
    }
    

	printf("Find all entries with id<5\n");
	for (int i=0;i<5;i++)
	{
		Record record;
		record.id=i;
		int err=HT_GetAllEntries(*hi,(void*)&record.id);
	}
	printf("Now create a secondary index based on %s\n",sAttrName );
	sBuckets=5;
	int createErrorCode=SHT_CreateSecondaryIndex(sfileName,sAttrName,sAttrLength,sBuckets,fileName);
	if (createErrorCode<0)
	{
		printf("SecondaryIndex creation FAILED\n");
		return -1;
	}	
	SHT_info* shi=SHT_OpenSecondaryIndex(sfileName);
	if(shi==NULL)
	{
		printf("SecondaryIndex open FAILED\n");
	}

	char const* const inputfile2 = argv[2]; 
    FILE* file2 = fopen(inputfile2, "r");

    if (file2==NULL)
    {
    	printf("error opening file %s\n",inputfile2 );
    	return -1;
    }

    printf("Now inserting records from file %s(%d)\n",inputfile2,file2 );
    counter=0;
    char line2[256];

    while (fgets(line2, sizeof(line2), file2)) 
    {
    	for (int i = 0; i < sizeof(line); ++i)
		{
			if (line[i]=='\n')
			{
				line[i]='\0';
			}
		}

        Record record;
       	token = strtok(line2, s);
   		counter=0;
		while( token != NULL ) 
		{

	    	if (counter==0)
	    	{
	    		char *temp=malloc(strlen(token));
	    		int currid=-1;
	    		for (int i = 0; i < strlen(token)-1; ++i)
	    		{
	    			temp[i]=token[i+1];
	    		}
	    		currid=atoi(temp);
	    		//printf("[%d]-",currid );
	    		record.id=currid;
	    	}
	    	else if (counter==3)
	    	{
	    		char *temp=malloc(strlen(token)-2);
	    		for (int i = 0; i < strlen(token)-3; ++i)
	    		{
	    			temp[i]=token[i+1];
	    		}
	    		//printf("[%s]\n",temp );
	    		sprintf(record.address,"%s",temp);
	    	}
	    	else
	    	{
				char *temp=malloc(strlen(token)-1);
	    		for (int i = 0; i < strlen(token)-2; ++i)
	    		{
	    			temp[i]=token[i+1];
	    		}
	    		//printf("[%s]-",temp );
	    		if (counter==1)
	    		{
	    			sprintf(record.name,"%s",temp);
	    		}
	    		else
	    		{
	    			sprintf(record.surname,"%s",temp);	
	    		}
	    	}
	    	counter++;
	    	token = strtok(NULL, s);
		}
		int bl=HT_InsertEntry(*hi,record);
		SecondaryRecord srecord;
		srecord.record=record;
		srecord.blockId=bl;
		int sbl=SHT_SecondaryInsertEntry(*shi,srecord);
		totalrecs++;
    }

    printf("Give a name to be searched for in the secondary index: ");
    char searchname[15];

    scanf("%s",searchname);

    Record record;
    strcpy(record.name,searchname);
	int err=SHT_SecondaryGetAllEntries(*shi,*hi,(void*)record.name);

	return err;
}   