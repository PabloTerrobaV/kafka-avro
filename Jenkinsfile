pipeline {
    agent any

    // Definición de variables de entorno, que se usarán en los comandos
    environment {
        // URL del Schema Registry, se usa en el script Python de validación
        SCHEMA_REGISTRY_URL = 'http://schema-registry:8081'
        // El subject que se usará para consultar la configuración de compatibilidad en Schema Registry
        SUBJECT_NAME = 'orders-value'
        // URL del repositorio Git donde se encuentra el esquema actualizado
        GITHUB_REPO_URL = 'https://github.com/PabloTerrobaV/kafka-avro.git'
        // Rama del repositorio
        GITHUB_BRANCH = 'main'
        // Ruta relativa del esquema dentro del proyecto (nuevo esquema que se obtendrá del repo)
        SCHEMA_PATH = 'common/src/main/avro/Order.avsc'
    }

    stages {
        stage('Descargar versión antigua del esquema') {
            steps {
                echo 'Descargando versión antigua del esquema desde Schema Registry...'
                // Se descarga el esquema antiguo usando el endpoint /versions/latest (sin /schema)
                // Luego se utiliza jq para extraer el campo "schema" y guardarlo en old_schema.avsc
                sh """
                curl -v ${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT_NAME}/versions/latest | jq -r .schema > old_schema.avsc || {
                    echo "Error al descargar el esquema antiguo"
                    exit 1
                }
                """
            }
        }

        stage('Obtener nueva versión del esquema') {
            steps {
                echo 'Descargando nueva versión del esquema desde GitHub...'
                // Se realiza el checkout del repositorio para obtener el archivo actualizado
                checkout([
                    $class: 'GitSCM',
                    branches: [[name: "${GITHUB_BRANCH}"]],
                    userRemoteConfigs: [[url: "${GITHUB_REPO_URL}"]]
                ])
                // Se copia el esquema actualizado a new_schema.avsc
                sh "cp ${SCHEMA_PATH} new_schema.avsc || { echo 'Error al copiar el nuevo esquema'; exit 1; }"
            }
        }

        stage('Inspeccionar esquemas') {
            steps {
                // Se verifica que ambos archivos existan y se muestran sus contenidos
                sh '''
                echo "🔍 Verificando existencia y contenido de los archivos AVSC..."

                if [ -f old_schema.avsc ]; then
                  echo "✅ old_schema.avsc existe:"
                  cat old_schema.avsc
                else
                  echo "❌ old_schema.avsc no se encuentra"
                fi

                if [ -f new_schema.avsc ]; then
                  echo "✅ new_schema.avsc existe:"
                  cat new_schema.avsc
                else
                  echo "❌ new_schema.avsc no se encuentra"
                fi
                '''
            }
        }

        stage('Comparar esquemas y detectar cambios') {
            steps {
                echo 'Comparando esquemas y detectando cambios...'
                // Se muestra información de depuración: lista de archivos en scripts/ y contenido de los esquemas
                sh '''
                echo "[DEBUG] Archivos disponibles en scripts/"
                ls -l scripts

                echo "[DEBUG] Contenido de old_schema.avsc:"
                cat old_schema.avsc || echo "No se encontró old_schema.avsc"

                echo "[DEBUG] Contenido de new_schema.avsc:"
                cat new_schema.avsc || echo "No se encontró new_schema.avsc"

                echo "[DEBUG] Ejecutando script de comparación..."
                python3 scripts/compare_schemas.py old_schema.avsc new_schema.avsc > schema_diff.txt || {
                    echo "[ERROR] Error durante la comparación"
                    cat schema_diff.txt || echo "schema_diff.txt no se creó"
                    exit 1
                }

                echo "[DEBUG] Contenido de schema_diff.txt:"
                cat schema_diff.txt
                '''
                script {
                    def changes = readFile('schema_diff.txt')
                    echo "Cambios detectados:\n${changes}"
                }
            }
        }

        stage('Validar compatibilidad del esquema') {
            steps {
                echo 'Validando compatibilidad del esquema...'
                // Se ejecuta el script Python de validación que:
                // 1. Consulta la configuración de compatibilidad (usando el Schema Registry).
                // 2. Analiza los cambios entre el esquema anterior y el nuevo.
                // 3. Valida la compatibilidad y, en caso de incompatibilidad, fuerza la salida con exit code 1.
                sh '''
                python3 scripts/validate_compatibility.py old_schema.avsc new_schema.avsc || {
                    echo "[ERROR] La validación de compatibilidad ha fallado"
                    exit 1
                }
                '''
            }
        }
    }

    post {
        success {
            echo "✅ Proceso completado exitosamente. Los esquemas fueron descargados, comparados y validados."
        }
        failure {
            echo "❌ Proceso fallido. Revisar los logs para más detalles."
        }
    }
}
