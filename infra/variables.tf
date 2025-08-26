# infra/variables.tf

# Define o nome do projeto. Usado para nomear os contêineres e outros recursos
variable "project_name" {
  type        = string
  default     = "pc-monitoring"
  description = "Nome do projeto para prefixar os recursos"
}

# Define a porta do servidor Prefect Orion
variable "prefect_port" {
  type        = number
  default     = 4200
  description = "Porta para acessar a UI do Prefect Orion"
}

# Em um projeto completo, você teria variáveis para:
# - Versões das imagens Docker
# - Credenciais de banco de dados
# - Portas de outros serviços (Superset, Grafana)