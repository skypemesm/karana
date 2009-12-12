
-------------------------- KARANA v1.0 ------------------------------

1. About the software:
----------------------
	Karana is a Lightweight SQL interpreter, which accepts simple SQL queries and runs them. It works on a simple SQL grammar as described in http://faculty.cs.tamu.edu/chen/courses/608/2009/project/TinySQL.pdf
It is a command line tool, which accepts and processes the queries and provides appropriate output.

	Karana runs ONLY on Windows platforms.


2. How to install the software:
--------------------------------
	This is a standalone software, and does not require any installation.

	The executable is provided in the zip file. It is named as karana.exe. To run the executable, simply go to command line, move to the appropriate folder containing the exe, and then type 'karana' at the command prompt. Typing 'karana --h' will show you the basic usage.


3. How to build from the source:
--------------------------------	
	The commented source code is also available in the zip file.  Building from the source on a Windows platform is a pretty straightforward process. Please do the following in the specified order:

a] Install and run Microsoft Visual C++ 2010 on your system.
b] Create an Empty project and name it Karana.
c] Copy all the files and folder Test to the folder {My Documents     /UserName}/Visual C++/Projects/Karana/Karana.

d] Drag and drop the following files into the 'Header Files' zone:
	karana_part1.cpp,
	karana_part2.cpp,
	Markup.h,
	one.tab.h,
	QueryManager.cpp,
	StorageManager.h.

e] Open Project > Properties > Configuration Properties > General.
	Click on Character Sets and Select "Use Unicode Character Set".

f] Next Select C/C++ > Preprocessor.
	Click on Preprocessor Definitions and Select "Edit" in the dropdown. Type in "MARKUP_STL" and click on Apply button.

g] Next Select C/C++ > General and Click on Additional Include Directories. Add the path to Test directory in Visual C++ Project Karana folder as in step c].

h] Build the project in Visual C++ by pressing F5.


	For guide about the usage of Karana, please refer to the User Guide in the report.



4. Acknowledgements:
--------------------
	Database Group, Dr. Jianer Chen, Texas A&M University for providing a StorageManager library.
	http://www.firstobject.com/ for a CMarkup XML library.
