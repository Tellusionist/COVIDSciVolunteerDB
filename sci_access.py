from pyzipcode import ZipCodeDatabase
def get_zips(zip_codes, zip_radii=[], default_radius=30):
    '''
    Returns surrounding zip codes within a given radius in miles, default is 30
    '''
    # initialize the zipcode database
    zcdb = ZipCodeDatabase()
    
    # append zeros if not in 5 digit format
    zip_formatted = [z.zfill(5) for z in zip_codes]
    
    # catch if null radius passed
    if len(zip_radii) == 0:
        zip_radii = [default_radius]
    elif zip_radii[0] == '':
        zip_radii = [default_radius]
        
    # check if each zipcode has a radius, if not fill with default radius value
    lst_diff = len(zip_codes) - len(zip_radii)
    if lst_diff > 0:
        if len(zip_radii) >0 and zip_radii[0] != '':
            zip_radii.extend([zip_radii[0]] * lst_diff)
        else:
            zip_radii.extend([default_radius] * lst_diff)

    zip_dic = {zip_formatted[i]: zip_radii[i] for i in range(len(zip_formatted))} 

    zip_output = []
    for z, r in zip_dic.items():
        try:
            zip_output.extend([z.zip for z in zcdb.get_zipcodes_around_radius(z,r)])
        except:
            pass
    return zip_output

from datetime import datetime

def parse_dts(date_str):
    '''
    Attempts to parse dates in 4 main formats and also handles null dates
    '''
    if date_str == '':
        return datetime(1990, 1, 1, 1, 1, 1, 1)
    
    for fmt in ('%m/%d/%Y %H:%M:%S', '%m/%d/%Y %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S','%Y-%m-%d %H:%M:%S.%f'):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass

import boto3

def upload_aws(df, filename, akey, skey):
    '''
    Helper function to upload files to AWS.
    '''
    from botocore.exceptions import NoCredentialsError

    ACCESS_KEY = akey
    SECRET_KEY = skey
    df.to_csv(filename, index=False)

    def upload_to_aws(local_file, bucket, s3_file):
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)

        try:
            s3.upload_file(filename, 'covidscivolunteerspublic', filename)
            print("Upload Successful")
            return True
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False


    uploaded = upload_to_aws('local_file', 'bucket_name', 's3_file_name')