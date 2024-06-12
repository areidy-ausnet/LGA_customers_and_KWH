# LGA_customers_and_KWH
Calculates approximate LGA customer numbers and KWH values so councils can report greenhouse gases.

Requirements:
- MAP of tariffs to categories e.g. residential, commercial, industrial
- Map of nmis to substations
- Map of substations to LGA regions
- LGA boundary shape files
- Postcode boundary files

# Instructions
1. Download the latest LGA and postcode shapefile data from the ABS (see input folder for url)
2. Change dates in python files in section `if __name__ == "__main__":`
3. change column "LGA_NAME23" and shapefile name if required
4. Install python libs by running `pip install -r requirements.txt` 
5. Run two python scrips
5. Get outputs and aggregate data with less than 10 nmi 




