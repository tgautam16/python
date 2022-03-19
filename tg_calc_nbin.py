//To calculate maximum number of bins by taking frequency from .par file*
// and sampling time from header .gmrt_hdr file, output is in the nbin.txt*

#include<iostream>
#include<fstream>
#include<cmath>
using namespace std;
int main()
{
  fstream par,hdr;
  par.open("ephem-trial.par");
  hdr.open("01_NGC6440_cdp_02sep2018.gmrt_hdr");
  ofstream nbin_out;
  nbin_out.open("nbin.txt");
  int nbin, i,j,k,x, a;
  string str_freq = "F0";
  string str_samp = "Sampling";
  string text1,text3;
  long double text2,text4,freq, tsamp, nbin2,period;
  while(!par.eof())
    {
      par>>text1;
      if(text1==str_freq)
	{
	  par>>text2;
	  freq=text2;
	  cout<<"Frequency is: "<<freq<<endl;
	}
    }
  period = 1/freq;

  while(!hdr.eof())
    {
      hdr>>text3;
      // cout<<text3<<endl;
      if(text3 == str_samp)
	{

	  hdr>>text3;
	  hdr>>text3;
	  hdr>>text4;
	  tsamp=text4;
	  cout<<"Sampling time is: " <<tsamp<<endl;
	}
    }
  tsamp=tsamp*pow(10,-6);   //converting to seconds
  cout<<"period is " <<period<<endl;
  nbin = period/tsamp;        //should be the nearest integer now.
  cout<<"number of bin originaly "<<nbin<<endl;
  cout<<"Sampling time actual is: "<<tsamp<<endl;
  // nbin = 2560;
  for(i=nbin;i>0;i--)
    {
      for(j=15;j>0;j--)
	{
	  x=pow(2,j);
	  if(i==x)
	    {
	      nbin=i;
	      nbin_out<<nbin;
	      cout<<"greatest power of 2 for nbin"<<nbin<<endl;
	      goto a;
	    }
	}
    }
 a: 
  return 0;
}
