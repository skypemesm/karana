#include<Windows.h>
#include<iostream>
#include<stdio.h>
#include<conio.h>
#include<string>
#include<sstream>
#include<fstream>
#include<string.h>
#include<map>
#define MAX_SIZE 200*1024 //200 kb
using namespace std;

void Callback(string,FILE*,WCHAR*);
int ReadFile(FILE*,char*);
string UpToLow(string str);
string proj_directory;
int main()
{
	
	FILE* fp;
	FILE* fn;
	FILE* fpr;
	cout<<">Welcome to feature generation and stop word removal module. This will remove query parameters in URL.";
	printf("\n>Enter project directory\n>");
	cin>>proj_directory; 
	if(*(proj_directory.end()-1)!='\\')
		proj_directory.append("\\");
	WIN32_FIND_DATA FindData;
	HANDLE file=INVALID_HANDLE_VALUE;
	string pos_dir=proj_directory+"positive\\*.txt";
	string neg_dir=proj_directory+"negative\\*.txt";
	string pred_dir=proj_directory+"predict\\*.txt";
	fp=fopen((proj_directory+"positive\\positive_data.data").c_str(),"w");
	fn=fopen((proj_directory+"negative\\negative_data.data").c_str(),"w");
	fpr=fopen((proj_directory+"predict\\classify_data.data").c_str(),"w");
	//for positive files//
	std::wstring wpos(pos_dir.begin(),pos_dir.end());	
	file=FindFirstFile(wpos.c_str(),&FindData);
	if(file==INVALID_HANDLE_VALUE)
	{
		printf("The error is: %u",GetLastError());
		return -1;
	}
	wprintf(L">reading %s\n",FindData.cFileName);
	Callback(proj_directory+"positive\\",fp,FindData.cFileName);
	while(FindNextFile(file,&FindData))
	{
		wprintf(L">reading %s\n",FindData.cFileName);
	Callback(proj_directory+"positive\\",fp,FindData.cFileName);
	}
	//for negative files//
	std::wstring wneg(neg_dir.begin(),neg_dir.end());	
	file=FindFirstFile(wneg.c_str(),&FindData);
	if(file==INVALID_HANDLE_VALUE)
	{
		printf(">The error is: %u",GetLastError());
		return -1;
	}
	wprintf(L">reading %s\n",FindData.cFileName);
	Callback(proj_directory+"negative\\",fn,FindData.cFileName);
	while(FindNextFile(file,&FindData))
	{
		wprintf(L">reading %s\n",FindData.cFileName);
		Callback(proj_directory+"negative\\",fn,FindData.cFileName);
	}
	//for predictive files//
	std::wstring wpred(pred_dir.begin(),pred_dir.end());	
	file=FindFirstFile(wpred.c_str(),&FindData);
	if(file==INVALID_HANDLE_VALUE)
	{
		printf(">The error is: %u",GetLastError());
		return -1;
	}
	wprintf(L">reading %s\n",FindData.cFileName);
	Callback(proj_directory+"predict\\",fpr,FindData.cFileName);
	while(FindNextFile(file,&FindData))
	{
		wprintf(L">reading %s\n",FindData.cFileName);
		Callback(proj_directory+"predict\\",fpr,FindData.cFileName);
	}
	fclose(fp);
	fclose(fn);
	fclose(fpr);
	getch();
	return 0;
}

void Callback(string dir,FILE* f,WCHAR* X)
{
	std::ifstream ifs(proj_directory+"stopwords.txt");
	map<string,int>stopwords;
	string t;
	char stopbuf[100];
	while(ifs.good())
	{
		ifs.getline(stopbuf,100);
		t.assign(stopbuf);
		stopwords.insert(pair<string,int>(UpToLow(t),1));
	}
	ifs.close();
	std::wstring wstr(X);
	char *buf=new char[MAX_SIZE + 100];
	string file(wstr.begin(),wstr.end());
	file=dir+file;
	FILE* fp;
	fp=fopen(file.c_str(),"r");
	int ret=0;
	char* temp=NULL;
	char* pos=buf;
	string x;
	do
	{
		ret=ReadFile(fp,buf);
		buf[ret]=0;
		pos=buf;
		x.assign("");
		while(true)
		{
			if(strstr(pos,"http://")==pos)
			{
				pos=pos+7*sizeof(char);//for rejecting "http://"
			}
			//if(strpbrk(pos,"/")!=NULL)			
			//	pos=strpbrk(pos,"/")+1;
			while((temp=strpbrk(pos,"-&./?=,;:\n"))!=NULL && *temp!='\n')
			{
				if(*(pos-sizeof(char))=='=')
				{
					pos=temp+sizeof(char);
					continue;
				}

				//fwrite(pos,sizeof(char),temp-pos,f);
				//fprintf(f,",");
				t.assign(pos,temp-pos);
				if(stopwords.find(t)==stopwords.end())
				{
				x.append(pos,temp-pos);
				x.append(",");
				}
				pos=temp+sizeof(char);
			}
			
		if(temp==NULL)
		{
			if(*(pos-sizeof(char))!='=')
			{
				if(stopwords.find(t)==stopwords.end())
				x.append(pos,ret-(pos-buf));
				else
					x.erase(x.end()-1);
				
			//fwrite(pos,sizeof(char),ret-(pos-buf),f);
			//fprintf(f,"\n");
			}
			else
				x.erase(x.end()-1);
				x.append("\n");
			break;
		}
		else
		{
		if(*(pos-sizeof(char))!='=')
		{
			if(stopwords.find(t)==stopwords.end())
			x.append(pos,temp-pos);
			else
				x.erase(x.end()-1);
			
		/*fwrite(pos,sizeof(char),temp-pos,f);*/
		/*fprintf(f,"\n");*/
		}
		else
			x.erase(x.end()-1);
			pos=temp+sizeof(char);
			x.append("\n");
		}
	 
		}
		fwrite(x.c_str(),sizeof(char),x.size(),f);
	}while(ret>=MAX_SIZE);
	fclose(fp);
	remove(file.c_str());
	
}

int ReadFile(FILE* f,char* buf)
{
	char c;
	int i=0;
	int cnt=fread(buf,sizeof(char),MAX_SIZE,f);
	while((c=fgetc(f))!='\n' && c!=EOF)
	{
		buf[cnt+i++]=c;
	}
	return cnt+i;
}

string UpToLow(string str) {
    for (int i=0;i<strlen(str.c_str());i++) 
        if (str[i] >= 0x41 && str[i] <= 0x5A) 
            str[i] = str[i] + 0x20;
    return str;
}