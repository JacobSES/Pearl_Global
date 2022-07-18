import smtplib
import email
from datetime import date
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase    

class EMAIL():
    
    def __init__(self, sender_email, sender_password):
        
        self.smtp_info = dict({"smtp_server" : "smtp.office365.com", # SMTP Server Address
                        "smtp_user_id" : sender_email, 
                        "smtp_user_pw" : sender_password, 
                        "smtp_port" : 587}) # SMTP server Port Number 

    def create_message(self, sender_email, receiver_email, today_report):
        title = "TDU-04 Site Production Report " + str(date.today())
        content = "Please find the attached site production report."

        self.msg_dict = {
            'application' : {'maintype' : 'application', 'subtype' : 'octect-stream', 'filename' : today_report} # attachement file
        }

        self.msg = MIMEText(_text = content, _charset = "UTF-8") 
        self.multi_msg = self.make_multimsg(self.msg_dict)
        self.multi_msg['subject'] = title  
        self.multi_msg['from'] = sender_email  
        self.multi_msg['to'] = receiver_email
        self.multi_msg.attach(self.msg)
        
    def send_email(self):
        with smtplib.SMTP(self.smtp_info["smtp_server"], self.smtp_info["smtp_port"]) as server:
              
            server.starttls() 
            server.login(self.smtp_info["smtp_user_id"], self.smtp_info["smtp_user_pw"])
            response = server.sendmail(self.multi_msg['from'], self.multi_msg['to'], self.multi_msg.as_string()) 
        if not response:
            print('successfully send email!')
        else:
            print(response)
            
            
    def make_multimsg(self, msg_dict):
        multi = MIMEMultipart(_subtype='mixed')
        
        for key, value in msg_dict.items():
            if key == 'text':
                with open(value['filename'], encoding='utf-8') as fp:
                    msg = MIMEText(fp.read(), _subtype=value['subtype'])
            elif key == 'image':
                with open(value['filename'], 'rb') as fp:
                    msg = MIMEImage(fp.read(), _subtype=value['subtype'])
            elif key == 'audio':
                with open(value['filename'], 'rb') as fp:
                    msg = MIMEAudio(fp.read(), _subtype=value['subtype'])
            else:
                with open(value['filename'], 'rb') as fp:
                    msg = MIMEBase(value['maintype'],  _subtype=value['subtype'])
                    msg.set_payload(fp.read())
                    encoders.encode_base64(msg)
            msg.add_header('Content-Disposition', 'attachment', filename=value['filename'])
            multi.attach(msg)
        return multi