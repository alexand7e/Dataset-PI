from datetime import datetime
from dateutil.relativedelta import relativedelta

class GeradorDePeriodos:
    def __init__(self):
        self.duracao_dos_periodos = {
            'anual': 5 * 12,        # 5 anos por período, cada ano 12 meses
            'mensal': 36,           # 36 meses por período
            'trimestral': 12 * 3    # 12 trimestres por período, cada trimestre 3 meses
        }

    def analisar_data(self, data_str):
        if self.periodicidade == 'anual':
            return datetime.strptime(data_str[:4], '%Y')
        elif self.periodicidade in ['mensal', 'trimestral']:
            return datetime.strptime(data_str, '%Y%m')

    def formatar_data(self, data):
        if self.periodicidade == 'anual':
            return data.strftime('%Y')
        elif self.periodicidade in ['trimestral', 'mensal']:
            return data.strftime('%Y%m')

    def obter_periodo(self, periodicidade, inicio, fim, last: bool = None):
        if last:
            return "p/last"
        
        self.periodicidade = periodicidade
        data_inicio = self.analisar_data(inicio)
        data_fim = self.analisar_data(fim)

        periodos = []
        inicio_atual = data_inicio

        while inicio_atual <= data_fim:
            meses_a_adicionar = self.duracao_dos_periodos[self.periodicidade]
            fim_atual = inicio_atual + relativedelta(months=meses_a_adicionar) - relativedelta(days=1)
            
            if self.periodicidade == 'trimestral':
                # Ajustar para o final do trimestre (março, junho, setembro, dezembro)
                fim_atual = fim_atual.replace(month=(fim_atual.month // 3) * 3, day=1) + relativedelta(months=1) - relativedelta(days=1)
            
            if fim_atual > data_fim:
                fim_atual = data_fim

            periodos.append(f'/p/{self.formatar_data(inicio_atual)}-{self.formatar_data(fim_atual)}')
            inicio_atual = fim_atual + relativedelta(days=1)

        return periodos 