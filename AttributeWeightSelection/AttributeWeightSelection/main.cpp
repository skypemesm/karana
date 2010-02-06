#include<Windows.h>
#include<iostream>
#include<fstream>
#include<stdio.h>
#include<stdlib.h>
#include<vector>
#include<map>
#include<set>
#include<hash_map>
#include<string>
#include<strsafe.h>
#include<math.h>
#include<conio.h>

#define BUFSIZE 1000
#define MAX_SIZE 200
#define MIN_DF_FOR_FEATURE 1

using namespace std;
using namespace stdext;

class indx{
public:
	int index;
	int DF;
};

class attribute{
public:
	string feature;
	double TF;
	int index;
	bool operator()(attribute X, attribute Y)
	{
		return X.index<Y.index;
	}
};

class data{
public:
	set<attribute,attribute>features;
	int label;
};

void Callback(string,WCHAR*,vector<data*>&,hash_map<string,indx>&,int);
void ComputeBinaryWeights(string,vector<data*>&,hash_map<string,indx>&);
void ComputeTFWeights(string,vector<data*>&,hash_map<string,indx>&);
void ComputeTFIDFWeights(string,vector<data*>&,hash_map<string,indx>&);
string IntToString(int);
string DoubleToString(double);
int positivecnt=0;
int negativecnt=0;
int predictcnt=0;
int main()
{
	string proj_directory;
	printf("\n>Welcome to attribute weight calculation module");
	printf("\n>Enter project directory\n>");
	cin>>proj_directory; 
	if(*(proj_directory.end()-1)!='\\')
		proj_directory.append("\\");
	WIN32_FIND_DATA FindData;
	HANDLE file=INVALID_HANDLE_VALUE;
	string pos_dir=proj_directory+"positive\\*.data";
	string neg_dir=proj_directory+"negative\\*.data";
	string pred_dir=proj_directory+"predict\\*.data";
	vector<data*>dataset;
	hash_map<string,indx>features;

	//for positive files//
	std::wstring wpos(pos_dir.begin(),pos_dir.end());	
	file=FindFirstFile(wpos.c_str(),&FindData);
	if(file==INVALID_HANDLE_VALUE)
	{
		printf("The error is: %u",GetLastError());
		return -1;
	}
	wprintf(L">reading %s\n",FindData.cFileName);
	Callback(proj_directory+"positive\\",FindData.cFileName,dataset,features,1);
	while(FindNextFile(file,&FindData))
	{
		wprintf(L">reading %s\n",FindData.cFileName);
	Callback(proj_directory+"positive\\",FindData.cFileName,dataset,features,1);
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
	Callback(proj_directory+"negative\\",FindData.cFileName,dataset,features,-1);
	while(FindNextFile(file,&FindData))
	{
		wprintf(L">reading %s\n",FindData.cFileName);
	Callback(proj_directory+"negative\\",FindData.cFileName,dataset,features,-1);
	}
	//for classification//
	std::wstring wpred(pred_dir.begin(),pred_dir.end());	
	file=FindFirstFile(wpred.c_str(),&FindData);
	if(file==INVALID_HANDLE_VALUE)
	{
		printf(">The error is: %u",GetLastError());
		return -1;
	}
	wprintf(L">reading %s\n",FindData.cFileName);
	Callback(proj_directory+"predict\\",FindData.cFileName,dataset,features,0);
	while(FindNextFile(file,&FindData))
	{
		wprintf(L">reading %s\n",FindData.cFileName);
	Callback(proj_directory+"predict\\",FindData.cFileName,dataset,features,0);
	}
	int selection;
	printf(">Choose the type of weighing scheme\n");
	printf(">1.Binary\n>2.TF\n>3.TFIDF\n");
	scanf("%d",&selection);
	switch(selection)
	{
	case 1: ComputeBinaryWeights(proj_directory,dataset,features);
				break;
	case 2: ComputeTFWeights(proj_directory,dataset,features);
				break;
	case 3:ComputeTFIDFWeights(proj_directory,dataset,features);
		break;
	default:break;
	}
	printf("\nNumber of positive examples: %d",positivecnt);
	printf("\nNumber of negative examples: %d",negativecnt);
	printf("\nNumber of predict examples: %d",predictcnt);
	getch();
}

void Callback(string dir,WCHAR* X,vector<data*>& Y,hash_map<string,indx>& Z,int label)
{
	std::wstring wstr(X);
	char buf[MAX_SIZE];
	string file(wstr.begin(),wstr.end());
	file=dir+file;
	std::ifstream ifs(file,ifstream::in);
	string temp;
	int pos=0;
	int tos=0;
	int sum=0;
	hash_map<string,indx>::iterator ter;
	hash_map<string,int>::iterator iter;
	while(ifs.good())
	{
		if(label==1)
			positivecnt++;
		else if(label==-1)
			negativecnt++;
		else
			predictcnt++;
		sum=0;
		data* _dt=new data();
		set<attribute,attribute>example;	
		hash_map<string,int>tempo;
		ifs.getline(buf,MAX_SIZE);
		temp.assign(buf);
		if(temp.length()<=1)
			continue;
		while((pos=temp.find_first_of(',',tos+1))!=string::npos)
		{
			if((iter=tempo.find(temp.substr(tos,pos-tos)))!=tempo.end())
			{
				iter->second++;
				sum++;
			}
			else 
			{
				if(temp.substr(tos,pos-tos)!="")
				{
				tempo.insert(pair<string,int>(temp.substr(tos,pos-tos),1));
				sum++;
				}
			}
			tos=pos+1;
			
		}
	
		if((iter=tempo.find(temp.substr(tos)))!=tempo.end())
		{
			iter->second++;
			sum++;
		}
			else
			{
				if(temp.substr(tos,pos-tos)!="")
				{
				tempo.insert(pair<string,int>(temp.substr(tos),1));
				sum++;
				}
		}

		int i;
		for(i=0,iter=tempo.begin();iter!=tempo.end();iter++,i++)
		{
			if(label!=0)
			{
			if((ter=Z.find(iter->first))!=Z.end())
				ter->second.DF++;
			else
			{
			
				indx i;
				i.DF=1;
				i.index=Z.size()+1;
				Z.insert(pair<string,indx>(iter->first,i));
			}
			}
			attribute a;
			a.feature=iter->first;
			a.TF=((double)iter->second)/(double)sum;
			a.index=((ter=Z.find(iter->first))!=Z.end())?ter->second.index:0;
			example.insert(a);		
		}	
		_dt->features=example;
		if(label==1)
		_dt->label=1;
		else if(label==-1)
		_dt->label=-1;
		else
		_dt->label=0;
		Y.push_back(_dt);
		pos=0;
		tos=0;
	}
}
void ComputeBinaryWeights(string directory,vector<data*>& Y,hash_map<string,indx>& X)
{
	string temp;
	string tempx;
	hash_map<string,indx>::iterator ter;
	std::ofstream ofs(directory+"train.dat");
	std::ofstream ofss(directory+"predict\\test.dat");
	for(int i=0;i<Y.size();i++)
	{
		if(Y[i]->label==1)
			temp.append("+1");
		else if(Y[i]->label==-1)
			temp.append("-1");
		else
			temp.append("0");
		tempx.append(" # ");
		for(set<attribute,attribute>::iterator it=Y[i]->features.begin();it!=Y[i]->features.end();it++)
		{
			temp.append(" ");
			if((ter=X.find(it->feature))!=X.end() && ter->second.DF>MIN_DF_FOR_FEATURE)
			{
			temp.append(IntToString(it->index));
			temp.append(":");
			temp.append("1");
			}
			tempx.append(it->feature);
			tempx.append(",");
		}
		if(*(tempx.end()-1)==',')
		tempx.erase(tempx.end()-1);
		if(Y[i]->label==0)
		ofss<<temp<<tempx<<"\n";
		else
		ofs<<temp<<tempx<<"\n";
		temp.clear();
		tempx.clear();

	}
	ofs.close();
	ofss.close();

}
void ComputeTFWeights(string directory,vector<data*>& Y,hash_map<string,indx>& X)
{
	string temp;
	string tempx;
	hash_map<string,indx>::iterator ter;
	std::ofstream ofs(directory+"train.dat");
	std::ofstream ofss(directory+"predict\\test.dat");
	for(int i=0;i<Y.size();i++)
	{
		if(Y[i]->label==1)
			temp.append("+1");
		else if(Y[i]->label==-1)
			temp.append("-1");
		else
			temp.append("0");
		tempx.append(" # ");
		for(set<attribute,attribute>::iterator it=Y[i]->features.begin();it!=Y[i]->features.end();it++)
		{
			temp.append(" ");
			if((ter=X.find(it->feature))!=X.end() && ter->second.DF>MIN_DF_FOR_FEATURE)
			{
			temp.append(IntToString(it->index));
			temp.append(":");
			temp.append(DoubleToString(it->TF));
			}
			tempx.append(it->feature);
			tempx.append(",");
		}
		if(*(tempx.end()-1)==',')
		tempx.erase(tempx.end()-1);
		if(Y[i]->label==0)
		ofss<<temp<<tempx<<"\n";
		else
		ofs<<temp<<tempx<<"\n";
		temp.clear();
		tempx.clear();

	}
	ofs.close();
	ofss.close();

}

void ComputeTFIDFWeights(string directory,vector<data*>& Y,hash_map<string,indx>& X)
{
	string temp;
	string tempx;
	hash_map<string,indx>::iterator ter;
	std::ofstream ofs(directory+"train.dat");
	std::ofstream ofss(directory+"predict\\test.dat");
	double Num;
	double DeNum;
	for(int i=0;i<Y.size();i++)
	{
		if(Y[i]->label==1)
			temp.append("+1");
		else if(Y[i]->label==-1)
			temp.append("-1");
		else
			temp.append("0");
		tempx.append(" # ");
		for(set<attribute,attribute>::iterator it=Y[i]->features.begin();it!=Y[i]->features.end();it++)
		{
			temp.append(" ");
			if((ter=X.find(it->feature))!=X.end() && ter->second.DF>MIN_DF_FOR_FEATURE)
			{
			temp.append(IntToString(it->index));
			temp.append(":");
			Num=(double)Y.size();
			DeNum=(ter=X.find(it->feature))==X.end()?0.0:(double)ter->second.DF;
			double IDF=log(Num/DeNum);
			temp.append(DoubleToString(it->TF*IDF));
			}
			tempx.append(it->feature);
			tempx.append(",");
		}
		if(*(tempx.end()-1)==',')
		tempx.erase(tempx.end()-1);
		if(Y[i]->label==0)
		ofss<<temp<<tempx<<"\n";
		else
		ofs<<temp<<tempx<<"\n";
		temp.clear();
		tempx.clear();

	}
	ofs.close();
	ofss.close();
}
string IntToString(int i)
{
	char *buffer=new char[33];
	itoa(i,buffer,10);
	string str(buffer);
	return str;
}

string DoubleToString(double x)
{
	char *buffer=new char[33];
	sprintf(buffer,"%4.4f",x);
	string str(buffer);
	return str;
}