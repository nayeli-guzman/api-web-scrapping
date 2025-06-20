from requests_html import HTMLSession
from bs4 import BeautifulSoup
import boto3
import uuid
import json
import traceback

def lambda_handler(event, context):
    try:
        # Crear una sesión HTML con requests-html
        session = HTMLSession()
        url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
        
        # Hacer la solicitud GET y ejecutar el JS (renderización)
        resp = session.get(url)
        resp.html.render(timeout=20, sleep=2)  # Espera para que se ejecute el JS

        # Parsear el contenido HTML de la página renderizada
        soup = BeautifulSoup(resp.html.html, 'html.parser')

        # Buscar la tabla en el HTML (esto puede variar dependiendo de la estructura)
        table = soup.find('table')  # Verifica si la tabla está en el HTML renderizado
        if not table:
            return {'statusCode': 404, 'body': json.dumps({'error': 'No se encontró la tabla'})}

        # Extraer los encabezados de la tabla
        headers = [th.text.strip() for th in table.find_all('th')]  # Obtener encabezados
        if not headers:
            return {'statusCode': 400, 'body': json.dumps({'error': 'La tabla no tiene encabezados'})}

        # Extraer las filas de la tabla (comenzando desde la fila 1 para omitir el encabezado)
        rows = []
        for tr in table.find_all('tr')[1:]:  # Omitir la fila de los encabezados
            cells = tr.find_all('td')
            if not cells:
                continue  # Ignorar filas vacías
            row_data = {}
            for i, cell in enumerate(cells):
                if i < len(headers):  # Asegúrate de que el índice sea válido
                    row_data[headers[i]] = cell.text.strip()
            rows.append(row_data)

        # Limitar a los primeros 10 registros
        rows = rows[:10]

        # Conectar a DynamoDB y guardar los datos
        dynamodb = boto3.resource('dynamodb')
        tabla_dynamo = dynamodb.Table('TablaWebScrapping')

        # Eliminar todos los registros previos
        scan = tabla_dynamo.scan()
        with tabla_dynamo.batch_writer() as batch:
            for item in scan['Items']:
                batch.delete_item(Key={'id': item['id']})

        # Insertar los primeros 10 registros en DynamoDB
        for i, row in enumerate(rows, start=1):
            row['#'] = i  # Asignar un número de fila
            row['id'] = str(uuid.uuid4())  # Generar un ID único para cada entrada
            tabla_dynamo.put_item(Item=row)

        # Devolver los datos como respuesta
        return {
            'statusCode': 200,
            'body': json.dumps(rows, ensure_ascii=False)
        }

    except Exception as e:
        # Log de error en caso de que ocurra una excepción
        error_log = {
            'error': str(e),
            'traceback': traceback.format_exc()
        }
        print(json.dumps(error_log))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
