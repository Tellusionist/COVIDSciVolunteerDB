import smtplib 
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.base import MIMEBase 
from email import encoders 
import os
from time import sleep
from datetime import datetime, timedelta

def email_results(name, email_add, df, epass):
    # save attachements
    csv_filename = 'Science_Volunteers.csv'
    df.to_csv(csv_filename, index=False)
    xl_filename = 'Science_Volunteers.xlsx'
    df.to_excel(xl_filename, sheet_name='Volunteers', index=False)
    
    fromaddr = 'covid19scivolunteers@gmail.com'
    toaddr = email_add

    # instance of MIMEMultipart 
    msg = MIMEMultipart() 

    # storing the senders email address   
    msg['From'] = fromaddr 

    # storing the receivers email address  
    msg['To'] = toaddr 

    # storing the subject  
    msg['Subject'] = "COVID-19 Volunteer Information"

    # string to store the body of the mail 
    body = """<p>Hello {},<br />&nbsp; &nbsp; &nbsp; We have received more volunteers in your area, please see the attached CSV and excel file for further details. We appreciate your help in reducing the impact of COVID-19! If you wish to change the frequency in which you receive this automated update, please reply to this email.</p>
<p>When contacting the individuals from this list, please let them know you received their contact information from the COVID-19 National Scientist Volunteer Database to maintain transparency. If you have any questions or would like assistance sorting and making use of the volunteers please send us an email. We are eager to support your efforts.</p>
<p>As a reminder, this data contains personal information and we count on you to keep this information private and to not distribute this data to unauthorized users. You can view our full terms of use here: <a href="https://covid19sci.org/terms-of-use/">https://covid19sci.org/terms-of-use</a></p>
<p>Thank you!<br />COVID-19 Scientist Volunteer Coordination Team</p>""".format(name)

    # attach the body with the msg instance 
    msg.attach(MIMEText(body, 'html')) 

    # attach the csv file
    attachment = open(csv_filename, "rb") 
    p = MIMEBase('application', 'octet-stream') 
    p.set_payload((attachment).read()) 
    encoders.encode_base64(p) 
    p.add_header('Content-Disposition', "attachment; filename= %s" % csv_filename) 
    msg.attach(p)
    attachment.close()
    
    # Attach the XL file
    attachment = open(xl_filename, "rb") 
    p = MIMEBase('application', 'octet-stream') 
    p.set_payload((attachment).read()) 
    encoders.encode_base64(p) 
    p.add_header('Content-Disposition', "attachment; filename= %s" % xl_filename) 
    msg.attach(p)
    attachment.close()

    # creates SMTP session 
    s = smtplib.SMTP('smtp.gmail.com', 587) 

    # start TLS for security 
    s.starttls() 
    
    # Authentication 
    s.login(fromaddr, epass) 

    # Converts the Multipart msg into a string 
    text = msg.as_string() 

    # sending the mail 
    s.sendmail(fromaddr, toaddr, text) 

    # terminating the session 
    s.quit() 
    
    # delete attachements
    try:
        os.remove(csv_filename)
    except PermissionError:
        sleep(5)
        os.remove(csv_filename)
        
    try:
        os.remove(xl_filename)
    except PermissionError:
        sleep(5)
        os.remove(xl_filename)

def heartbeat_email_check(epass):
    
    today = datetime.now()
    email_hb_file = 'last_hb_sent.txt'

    # If file not found, default to last sent 10 days ago to trigger heartbeat
    if not os.path.exists(email_hb_file):
        with open(email_hb_file, 'w') as f:
            adjusted_date = (today - timedelta(days=10)).strftime('%m/%d/%Y')
            f.write(adjusted_date)
    
    # Check when heartbeat was last sent
    with open(email_hb_file, 'r') as f:
        last_sent = f.read()
    last_sent = datetime.strptime(last_sent, '%m/%d/%Y')

    # Don't send heartbeat unless 7 days have passed
    if today < (last_sent + timedelta(days=7)):
        return

    # build and send email
    fromaddr = 'covid19scivolunteers@gmail.com'
    toaddr = 'covid19scivolunteers@gmail.com'

    msg = MIMEMultipart() 
    msg['From'] = fromaddr 
    msg['To'] = toaddr 
    msg['Subject'] = "NSVD Email Bot Heartbeat"
    body = 'Heartbeat email to keep Gmail connection live. Please ignore.'

    msg.attach(MIMEText(body, 'html')) 
    s = smtplib.SMTP('smtp.gmail.com', 587) 
    s.starttls() 
    s.login(fromaddr, epass) 
    text = msg.as_string() 
    s.sendmail(fromaddr, toaddr, text) 
    s.quit() 
    
    # Update heartbeat file with last sent date
    with open(email_hb_file, 'w') as f:
        sent_date = today.strftime('%m/%d/%Y')
        f.write(sent_date)
    
    return