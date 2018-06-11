# Agni GCR Attendance #

Stand-alone program for generating Agni GCR webinar attendance reports.
This will do the following:
* Read Agni GCR webinar attendee reports csv files
* Save the data to sqlite db file
* Generate a consolidated email-wise attendance report

### Using the program ###
* Download ```agni_gcr_attendance.exe``` from [https://github.com/ramprax/agni-gcr-attendance/releases](https://github.com/ramprax/agni-gcr-attendance/releases)
* Save it to any folder of your choice
* Copy the webinar attendee reports csv files(downloaded from zoom website) to the above folder
* Run ```agni_gcr_attendance.exe``` by double clicking it
* On first run, this will create a file ```agni-gcr.db```
* On subsequent runs, this file will be kept updated and used
* A directory `output` will be created where the consolidated reports will be saved
* You can look at the contents using of ```agni-gcr.db``` using sqlite browser like [https://sqlitebrowser.org/](https://sqlitebrowser.org/)

### Project setup ###

* Clone project with ```git clone``` [https://github.com/ramprax/agni-gcr-attendance.git](https://github.com/ramprax/agni-gcr-attendance.git)
* Set up a virtualenv for the project and enter into the venv
* Run ```pip install -r dev-requirements.txt```

### Run locally in development mode ###

* Run ```python agni_gcr_attendance.py```

### Build the executable ###

* For folder-bundle, run ```pyinstaller agni_gcr_attendance.py```
* For a single file bundle, run ```pyinstaller --onefile agni_gcr_attendance.py```
* The executable will be for the target environment which is identical to your development environment
