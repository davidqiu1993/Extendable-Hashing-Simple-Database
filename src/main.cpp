#include "General.h"

using namespace exth;

#define LEAST_8
//#define LEAST_128
//#define MOST_8
//#define MOST_128

int main(int argc, char *argv[])
{
	string temp(argv[1]);

	string tblFilePath = temp + "\\lineitem.tbl";
	string dbContentFilePath = temp + "\\bucket.tbl";
	string dbIndexFilePath = temp + "\\hashindex.out";
	string TestInputFilePath = temp + "\\testinput.in";
	string TestoutputFilePath = temp + "\\testoutput.out";

    int start_tick = clock();
#ifdef LEAST_8
	printf("Now running: LEAST_8.\n");
	LowHashManagement(8, tblFilePath, dbContentFilePath, dbIndexFilePath, TestInputFilePath, TestoutputFilePath);
#endif // LEAST_8
#ifdef LEAST_128
	printf("Now running: LEAST_128.\n");
	LowHashManagement(128, tblFilePath, dbContentFilePath, dbIndexFilePath, TestInputFilePath, TestoutputFilePath);
#endif // LEAST_128
#ifdef MOST_8
	printf("Now running: MOST_8.\n");
	HighHashManagement(8, tblFilePath, dbContentFilePath, dbIndexFilePath, TestInputFilePath, TestoutputFilePath);
#endif // MOST_8
#ifdef MOST_128
	printf("Now running: MOST_128.\n");
	HighHashManagement(128, tblFilePath, dbContentFilePath, dbIndexFilePath, TestInputFilePath, TestoutputFilePath);
#endif // MOST_128
  	int end_tick = clock();

#ifdef TEST_STATISTIC
	printf("Total input count: %d\n", ts_input_count);
	printf("Total input time: %dms\n", ts_input_time);
	printf("Total output count: %d\n", ts_output_count);
	printf("Total output time: %dms\n", ts_output_time);
	printf("Total I/0 count: %d\n", ts_input_count + ts_output_count);
	printf("Total I/O time: %dms\n", ts_input_time + ts_output_time);
#endif // TEST_STATISTIC
	printf("Total run time: %dms\n", end_tick - start_tick);
	printf("Proportion of I/O time: %.2lf%\n", 
		(((double)ts_input_time + (double)ts_output_time)/((double)end_tick - (double)start_tick))*100);
	
	printf("\n");
	system("pause");
}
