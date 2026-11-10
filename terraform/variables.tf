variable "alert_email" {
  description = "Correo para recibir alertas del presupuesto"
  type        = list(string)
}

variable "athena_bytes_scanned_cutoff" {
  description = "Limite de bytes escaneados por consulta en Athena"
  type        = number
  default     = 5368709120 # 5 GiB
}