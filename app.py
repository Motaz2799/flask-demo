from flask import Flask, make_response, request, send_file
import matplotlib.pyplot as plt
import requests
from Service.UploadExcel import store_excel
from flask_cors import CORS

from docxtpl import DocxTemplate ,InlineImage

import os
import comtypes.client
import docx
import Service.Scoring as scoring
import Service.ImageInterface as imageInterface
import Service.ImageInterface as ImageInterface
from py_eureka_client import eureka_client

import logging

logging.basicConfig(level=logging.INFO)
eureka_client.init(
     eureka_server="http://localhost:8761/eureka/",
     app_name="ETL-service",
     instance_port=5000,
     instance_host="localhost",

 )

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})


@app.route('/api/v1/generate_report/<int:app_id>/<string:type>', methods=['GET'])
def generate_report(app_id,type):
    auth_url = 'http://localhost:8080/api/v1/auth/authenticate'
    auth_payload = {'email': 'archimaster@orange.com', 'password': '123'}
    auth_response = requests.post(auth_url, json=auth_payload)
    if auth_response.status_code == 200:
        token = auth_response.json().get('token')
    else:
        return 'Failed to authenticate'

    headers = {'Authorization': f'Bearer {token}'}

    url = f'http://127.0.0.1:8080/api/v1/applications/{app_id}'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        variables_json = response.json()

    url_assessment = f'http://127.0.0.1:8080/api/v1/applications/{app_id}/assessment'
    response1 = requests.get(url_assessment, headers=headers)
    if response1.status_code == 200:
        variables_json1 = response1.json()

    url_interfaces = f'http://127.0.0.1:8080/api/v1/applications/{app_id}/interfaces'
    response2 = requests.get(url_interfaces, headers=headers)
    if response2.status_code == 200:
        variables_json2 = response2.json()


    image_path = imageInterface.get_interface(app_id)
    (jsonStrategie,image_path_strategie)=scoring.calculate_scores(app_id,2)

    comtypes.CoInitialize()
    doc = DocxTemplate("./Ressources/template_v2.docx")
    grouped_questions = {}

    for step in variables_json1['steps']:
        for category in step['categories']:
            if category['category'] in ['Application Overview', 'Availability & Business Continuity', 'End User Information', 'Infrastructure Supporting Components', 'Requirements & Constraints', 'Non-Production Information', 'Product & Vendor Information']:
                 if category['category'] in grouped_questions:
                     grouped_questions[category['category']].extend(category['questions'])
                 else:
                    grouped_questions[category['category']] = category['questions']
                    


    context = {
        'watermark':"Archimaster",
        'client':variables_json['appName'],
        'description': variables_json['appDescription'],
        'name': variables_json['appName'],
        'strategy':list(jsonStrategie.keys())[0],
        'detailed_startegy':list(jsonStrategie.values())[0],
        'migration_image':InlineImage(doc, image_path_strategie),        
        'relations_image':InlineImage(doc, image_path),        
        'interfaces': [
        {
            "source_name": interface['applicationSrc']['appName'],
            "target_name":  interface['applicationTarget']['appName'],
            "protocol": interface['protocol'],
            "internalOrExternal": interface['flow'],
            "batchOrRealTime":  interface['processingMode'],
            "frequency": interface['frequency'],

        }
        for interface in variables_json2
    ],


        'servers': [
        {
            "currentDiskGb": server['currentDiskGb'],
            "currentNumberOfCores":  server['currentNumberOfCores'],
            "currentRamGb": server['currentRamGb'],
            "datacenter": server['datacenter'],
            "environment":  server['environment'],
            "ipAddress": server['ipAddress'],
            "operatingSystem": server['operatingSystem'],
            "role":  server['role'],
            "serverName": server['serverName'],
            "type":  server['type'],

        }
        for server in variables_json['servers']
    ],
        'Contacts': [
        {
            'fullName': contact['fullName'],
            'title': contact['title'],
            'department': contact['department'], 
            'email': contact['email']
        }
        for contact in variables_json['contacts']
    ],
    'Assessment': {
    'id': variables_json1['id'],
    'assessment': variables_json1['assessment'],
    'createdAt': variables_json1['createdAt'],
    'note': variables_json1['note'],
    'groupedQuestions': grouped_questions
},
    'databases': [],
}
    for server in variables_json['servers']:
        server_name = server['serverName']
        for database in server['databaseList']:
            database_name = database['databaseName']
            context["databases"].append({
                "serverName": server_name,
                "databaseName": database_name
            })

    doc.render(context)
    doc.save("document_rempli.docx")
    if type=="pdf" :
        input_file = 'document_rempli.docx'
        output_file = 'document.pdf'
        doc = docx.Document(input_file)
        # Save the Word document as a PDF using Microsoft Word
        word = comtypes.client.CreateObject("Word.Application")
        docx_path = os.path.abspath(input_file)
        pdf_path = os.path.abspath(output_file)
        pdf_format = 17  # PDF file format code
        word.Visible = False
        in_file = word.Documents.Open(docx_path)
        in_file.SaveAs(pdf_path, FileFormat=pdf_format)
        in_file.Close()
        word.Quit()

        response = make_response(send_file('document.pdf'))
        response.headers['Content-Type'] = 'application/pdf'
        response.headers['Content-Disposition'] = 'attachment; filename=report2.pdf'

        return response
    
    file_path = os.path.abspath('document_rempli.docx')
    file_name = 'document_rempli.docx'
    response = make_response(send_file(file_path, as_attachment=True))
    response.headers['Content-Disposition'] = f'attachment; filename="{file_name}"'
    response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'

    return response


@app.route('/api/v1/strategy/<int:app_id>/<string:type>', methods=['GET'])
def get_strategies(app_id,type):
    if type=="detailed" :
        return scoring.calculate_scores(app_id,1)
    else :
        return scoring.calculate_scores(app_id,0)
    
@app.route('/api/v1/interface/<int:app_id>', methods=['GET'])
def get_imageInterface(app_id):
    image_path=ImageInterface.get_interface(app_id)
    return send_file(image_path, mimetype='image/png')

 



@app.route('/api/v1/upload', methods=['POST'])
def upload_excel():
    files = request.files.getlist('file')   
    return store_excel(files)    


#-------------------------------------------------------------------------

if __name__ == '__main__':
    app.run(debug=True)