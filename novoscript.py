import smtplib
import os

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders



def envia_email(email_from, email_to, password):
    
    email = email_from
    cliente = email_to
    msg = MIMEMultipart()

    msg['From'] = email
    msg['To'] = cliente
    msg['Subject'] = "Testando enviar emails com axexo"
    
    body = "\nCorpo da mensagem"

    msg.attach(MIMEText(body, 'plain'))

    source = r'cliente'
    file = os.listdir(source)
    filename = file[0]
    print(file)

    attachment = open(f"{source}/{file[0]}", "rb")


    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= %s" % filename)

    msg.attach(part)

    attachment.close()

    #s = smtplib.SMTP('smtp.gmail.com: 587')
    server = smtplib.SMTP('smtp.gmail.com: 587')
    server.starttls()
    server.login(email, password)
    text = msg.as_string()
    server.sendmail(email, cliente, text)
    server.quit()
    print('\nEmail enviado com sucesso!')
    # except:
    #    print("\nErro ao enviar email")
        
envia_email('costa.camila.ro@gmail.com', 'lila.costa.ro@gmail.com', 'Tom2910#')

# source = r'cliente'
# file = os.listdir(source)
# filename = file
# print(file)