International Trade Data Analysis (New Zealand) : 

 # Description of Column_name in datasets 
 Link to dataset : https://www.stats.govt.nz/assets/Uploads/International-trade/International-trade-June-2023-quarter/Download-data/international-trade-june-2023-quarter.zip

- Time_ref: This column represents a timestamp or date reference indicating when the trade transaction occurred.
- Account: This column refer to the account or entity involved in the trade transaction.
- Code: It represents product code.
- Country_Code: This column contains codes representing countries involved in the trade.
- Product type: This column describes the type or category of the traded products.
- Value: This column represents the monetary value associated with the trade transaction.
- Status: The "status" column indicates the status or outcome of the trade transaction (e.g., Free, Charged)


Q.1 Compute the following metrics for each country:
The sum of total export values.
The sum of total import values.
The difference between the sum of export and import values as ‘Trade balance’.
Now, Join  'revised_final" table with the 'country_final' table  & filter the result to select the peak countries with the highest trade balance.

Output : <img width="725" alt="1" src="https://github.com/Ujjwalkhadka47/Individual_Spark_Project_Fuse/assets/141219631/3c443b7a-a0bc-4647-819f-752cf5959a4e">



Q.2 Show the comparative study of the average transactions of 'account' column (export & import) for different product types (Goods & Services) from 2020 to 2023 combined in a table and show it in columns using Pivot DataFrame Transformations.

Output1 : <img width="820" alt="2a" src="https://github.com/Ujjwalkhadka47/Individual_Spark_Project_Fuse/assets/141219631/1c8c0078-7794-4b53-9a1f-d09592ecf4cf">


Output2 : <img width="820" alt="2b" src="https://github.com/Ujjwalkhadka47/Individual_Spark_Project_Fuse/assets/141219631/fad8b398-6792-4f41-b837-07df8795a0d0">


Output3 : <img width="820" alt="2c" src="https://github.com/Ujjwalkhadka47/Individual_Spark_Project_Fuse/assets/141219631/bcaf8d14-90ef-4c47-82dd-c18525d30cf9">


Output4 : <img width="820" alt="2d" src="https://github.com/Ujjwalkhadka47/Individual_Spark_Project_Fuse/assets/141219631/60c3f6ad-f9c1-4c6c-8eb6-831881bdd9b2">
