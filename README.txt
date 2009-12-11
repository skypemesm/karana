
-------------------------- KARANA v1.0 ------------------------------

1. About the software:

	Karana is a Lightweight SQL interpreter, which accepts simple XML queries and runs them. It works on a simple SQL grammar as described in http://faculty.cs.tamu.edu/chen/courses/608/2009/project/TinySQL.pdf
It is a command line tool, which accepts and processes the queries and provides appropriate output.

	Karana runs ONLY on Windows platforms.

2. How to install the software:

	This is a standalone software, and does not require any installation.

	The executable is provided in the zip file. It is named as karana.exe. To run the executable, simply go to command line, move to the appropriate folder containing the exe, and then type 'karana' at the command prompt. Typing 'karana --h' will show you the basic usage.

3. How to build from the source:
	
	The commented source code is also available in the zip file.  Building from the source on a Windows platform is a slightly complex process. Please do the following in the specified order:

a] Install Flex and Bison on your system.
b] Copy the files () to the folder 

	For guide about the usage of Karana, please refer to the User Guide in the report.

4. Acknowledgements:

	Database Group, Dr. Jianer Chen, Texas A&M University for providing a StorageManager library.
	http://www.firstobject.com/ for a CMarkup XML library.
