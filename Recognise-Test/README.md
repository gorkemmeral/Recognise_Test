# Recognise Bank – Head of Data Tech Assessment 
The evaluation process for the Head of Data role comprises two distinct segments. The specifics for
each part are outlined as follows:

# Part1
This segment of the case study is strategically crafted to assess the candidate's fundamental
programming expertise and their adeptness in data manipulation. Further details regarding the case
are elaborated below:

Task:
Referring to the input file "CC Application Lifecycle.csv,”, please manipulate the file to show
Application stages as column headings and the corresponding time of stage completion as
values for each customer ID. Structure the output as demonstrated in the file "Application
Lifecycle Output.csv." This data wrangling endeavour is expected to be executed using the
Python programming language.

Solution:
- transform.py (A Python script that transforms the input file into the output file where application statutes are seperate columns with timestamps)
- run_transform.py (A Python script that executes the transform.py file)

# Part2
This section of the case study is intentionally structured to evaluate the candidate's proficiency in
conceptualising and devising technical solutions. Further information regarding the case is provided
below:

Task:
The data team is currently tasked with constructing a data pipeline responsible for extracting
data from a MongoDB database, comprising 5 distinct collections, and seamlessly
transferring it into a Redshift data platform. Anticipating the integration of additional data
sources in the foreseeable future, the challenge lies in devising a comprehensive and
adaptable framework that fosters reusability, simplifies maintenance, and facilitates
modifications.

There are no requirements to delve deeply into the code itself; high-level class objects will suffice as
long as they effectively convey the narrative. When sharing your work, kindly ensure that the URL to
the GitHub repository is included, along with any other relevant artifacts presented in a format that is
easily accessible (such as Word or PDF).

Solution file:
- mongoDB_to_redshit_load.py (Class to connect to Mongo DB and Redshift, then drop, create, copy table)

Proposed Architecture
- proposed_architecture.pdf (Two diagrams to demonstrate the architecture proposal; Modern Data Stack approach and AWS environment only approach)

