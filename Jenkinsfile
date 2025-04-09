pipeline {
    agent any

    environment {
        SCHEMA_REGISTRY_URL = 'http://schema-registry:8081'
        SUBJECT_NAME = 'orders-value'
        GITHUB_REPO_URL = 'https://github.com/PabloTerrobaV/kafka-avro.git'
        GITHUB_BRANCH = 'main'
        SCHEMA_PATH = 'common/src/main/avro/Order.avsc'
    }

    stages {
        stage('Descargar versión antigua del esquema') {
            steps {
                echo 'Descargando versión antigua del esquema desde Schema Registry...'
                sh """
                curl -v ${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT_NAME}/versions/latest | \
                    jq -r .schema > old_schema.avsc || {
                        echo "Error al descargar el esquema antiguo"
                        exit 1
                    }
                """
            }
        }

        stage('Obtener nueva versión del esquema') {
            steps {
                echo 'Descargando nueva versión del esquema desde GitHub...'
                checkout([
                    $class: 'GitSCM',
                    branches: [[name: "${GITHUB_BRANCH}"]],
                    userRemoteConfigs: [[url: "${GITHUB_REPO_URL}"]]
                ])
                sh "cp ${SCHEMA_PATH} new_schema.avsc || { echo 'Error al copiar el nuevo esquema'; exit 1; }"
            }
        }

        stage('Inspeccionar esquemas') {
            steps {
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
    }

    post {
        success {
            echo "✅ Proceso completado exitosamente. Los esquemas fueron descargados y comparados."
        }
        failure {
            echo "❌ Proceso fallido. Revisar los logs para más detalles."
        }
    }
}