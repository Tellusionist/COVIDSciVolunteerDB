# COVIDSciVolunteerDB

Program to send email to local cordinators when there is a new volunteer in their area.

Program website: https://covid19sci.org/

  - Download the latest Volunteer Response data
  - Download the latest configuration 
  - Check against day frequency for that user if enough time has passed since previous run
  - Compare against previous output to see if new volunteers are found, email an updated list
    - Don't send an email if someone was removed, but update the last run date
    - Update the main table with the temp table if ther were new volunteers and send email
  - Update run nots to the configuration file
  - Add column [Unique ID] to "All Volunteers" backup if Volunteers_Subset_DB is deleted (don't email or upload)
