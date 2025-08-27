# infra/variables.tf

variable "project_name" {
  type        = string
  default     = "pc-monitoring"
  description = "Nome do projeto para prefixar os recursos"
}

variable "prefect_port" {
  type        = number
  default     = 4200
  description = "Porta para acessar a UI do Prefect Orion"
}