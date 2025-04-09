pipeline {
    agent any

    environment {
        SCHEMA_REGISTRY_URL = 'http://localhost:8081'
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
                curl -s ${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT_NAME}/versions/latest | \
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

        stage('Comparar esquemas y detectar cambios') {
            steps {
                echo 'Comparando esquemas y detectando cambios...'
                sh '''
                python3 scripts/compare_schemas.py old_schema.avsc new_schema.avsc > schema_diff.txt || {
                    echo "Error al comparar los esquemas"
                    exit 1
                }
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