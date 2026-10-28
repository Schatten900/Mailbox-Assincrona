#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <time.h>

#define N 50
#define LEITORES 10
#define ESCRITORES 5
#define INTERVALO_ENVELHECIMENTO 30

typedef enum {
    VAZIO,
    ESCREVENDO,
    DISPONIVEL,
    LENDO
} EstadoEmail;     


typedef struct{
    int id;
    char titulo[100];
    char descricao[500];
    int prioridade; // 1 - Baixa 2 - Medio 3 - Alta
    time_t instante_entrada; // Horário em que ficou disponível

} Email;

typedef struct{
    Email emails[N];                // Emails em si
    EstadoEmail estados[N];         // Controle dos estados do email
    int proxEmail;                  // Unicamente para depurar

    sem_t vagas_disponiveis;        // Permite multiplos leitores simultaneamente na caixa
    sem_t emails_disponiveis;       // Permite multiplos escritores simultaneamente na caixa
    sem_t mutex;                    // Permite modificar o ocupado sem race condition
    sem_t indices[N];               // Permite a modificacao dos indices simultaneamente

} CaixaEmail; 

typedef struct{
    CaixaEmail* caixa;
    int id;
} ArgumentosThread;


void * inserir_email(void * args);

void * ler_email(void * args);

int calcular_prioridade(Email email);


int main(){

    /* Inicializa os threads*/
    pthread_t escritores[ESCRITORES];
    pthread_t leitores[LEITORES];

    ArgumentosThread args_escritores[ESCRITORES];
    ArgumentosThread args_leitores[LEITORES];

    CaixaEmail caixa = {0};

    int i;
    
    /* Inicializa os semaforos da caixa de e-mail*/
    sem_init(&caixa.vagas_disponiveis,0,N);      // Iniciando quantas vagas estão disponiveis
    sem_init(&caixa.emails_disponiveis,0,0);     // Iniciando quantos emails para consumo
    sem_init(&caixa.mutex,0,1);                  // Iniciando o mutex

    for (i = 0; i < N; i++){
        sem_init(&caixa.indices[i],0,1);         // Inicia o mutex dos indices
    }

    /* Criação dos escritores*/
    for (i = 0; i < ESCRITORES; i++){

        args_escritores[i].caixa = &caixa;
        args_escritores[i].id = i;

        pthread_create(&escritores[i], NULL, inserir_email, &args_escritores[i]);
    }

    /* Criação dos leitores*/
    for (i = 0; i < LEITORES; i++){

        args_leitores[i].caixa = &caixa;
        args_leitores[i].id = i;

        pthread_create(&leitores[i], NULL, ler_email, &args_leitores[i]);
    }

    /* Aguarda os escritores*/
    for (i = 0; i < ESCRITORES; i++) {
        pthread_join(escritores[i], NULL);    // Evita que o programa acabe
    }

    /* Aguarda os leitores*/
    for (i = 0; i < LEITORES; i++) {
        pthread_join(leitores[i], NULL);    // Evita que o programa acabe
    }

    /* Destroi os semaforos*/

    sem_destroy(&caixa.emails_disponiveis);
    sem_destroy(&caixa.vagas_disponiveis);
    sem_destroy(&caixa.mutex);
    
    for(i = 0; i < N; i++){
        sem_destroy(&caixa.indices[i]);
    }

    return 0;
}


void * inserir_email(void * args){
    // Função que permite multiplos escritores acessarem indices não usados da caixa de e-mail para inserir
    ArgumentosThread *arg = (ArgumentosThread *)args;
    CaixaEmail *caixa = arg->caixa;
    int id = arg->id;

    while (1){
        int indice_uso = -1;

        sem_wait(&caixa->vagas_disponiveis); // Espera ter uma vaga disponivel

        sem_wait(&caixa->mutex);             // Pega o mutex 

        /* Procura algum indice disponivel */
        for(int i=0; i < N; i++ ){
            if (caixa->estados[i] == VAZIO){
                indice_uso = i;
                caixa->estados[i] = ESCREVENDO;

                printf("[ESCRITOR %d] Reservou indice %d\n", id, indice_uso);

                break;
            }
        }

        sem_post(&caixa->mutex);

        // Não encontrou indice disponivel
        if (indice_uso == -1){
            sem_post(&caixa->vagas_disponiveis);
            continue;
        }

        sem_wait(&caixa->indices[indice_uso]);  // Adquire o mutex daquele indice
        
        // Criação do e-mail
        Email email = {0};

        sem_wait(&caixa->mutex);
        email.id = caixa->proxEmail++;
        sem_post(&caixa->mutex);

        email.prioridade = (rand() % 3) + 1;

        snprintf(email.titulo, sizeof(email.titulo),
                "Email de teste %d", email.id);

        snprintf(email.descricao, sizeof(email.descricao),
                "Descricao do email %d", email.id);

        caixa->emails[indice_uso] = email;          // Insere o e-mail no indice

        sleep(20);              // Tempo para inserir no e-mail

        /* Publica o email para leitores */
        sem_wait(&caixa->mutex);

        caixa->emails[indice_uso].instante_entrada = time(NULL);    // Tempo em que foi inserido
        caixa->estados[indice_uso] = DISPONIVEL;

        sem_post(&caixa->emails_disponiveis);       // Libera uma entrada para leitura
        sem_post(&caixa->mutex);

        printf("[ESCRITOR %d] Email %d inserido no indice %d\n", id, email.id, indice_uso);

        // Libera apos modificar o estado
        sem_post(&caixa->indices[indice_uso]);      // Libera aquele indice para operações

        sleep(5);                                   // Tempo para sair
    }

    pthread_exit(0);

}

void * ler_email(void * args){
    // Função que permite multiplos leitores acessarem indices não usados da caixa de e-mail para leitura/remoção
    ArgumentosThread *arg = (ArgumentosThread *)args;
    CaixaEmail *caixa = arg->caixa;
    int id = arg->id;

    while (1){

        sem_wait(&caixa->emails_disponiveis); // Espera ter um email disponivel
        sem_wait(&caixa->mutex);              // Pega o mutex

        int indice_uso = -1;
        int maior_prioridade = -1;
        time_t tempo_mais_antigo = 0;

        for (int i=0; i < N; i++){
            if (caixa->estados[i] == DISPONIVEL){
                int prioridade_atual = calcular_prioridade(caixa->emails[i]);

                time_t entrada_atual = caixa->emails[i].instante_entrada;

                if (prioridade_atual > maior_prioridade || (prioridade_atual == maior_prioridade &&
                (indice_uso == -1 || entrada_atual < tempo_mais_antigo || (entrada_atual == tempo_mais_antigo &&
                caixa->emails[i].id < caixa->emails[indice_uso].id)))) {
                    maior_prioridade = prioridade_atual;
                    tempo_mais_antigo = entrada_atual;
                    indice_uso = i;
                } 
            }
        }

         // Não encontrou disponivel
        if (indice_uso == -1){
            sem_post(&caixa->mutex);
            sem_post(&caixa->emails_disponiveis);
            continue;
        }         

        caixa->estados[indice_uso] = LENDO;
        printf("[LEITOR %d] Lendo indice %d\n", id, indice_uso);

        sem_post(&caixa->mutex);

        // Realiza a leitura do e-mail
        sem_wait(&caixa->indices[indice_uso]);  // Reserva exclusão mutua naquele indice

        // Libera o indice 

        sleep(20);              // Tempo para Remover da caixa

        Email email = caixa->emails[indice_uso];

        caixa->emails[indice_uso] = (Email){0};  // Remove do array

        sem_wait(&caixa->mutex);

        caixa->estados[indice_uso] = VAZIO;        // Libera espaço para inserir email

        sem_post(&caixa->vagas_disponiveis);      // Libera vaga para escritor
        sem_post(&caixa->mutex);

        printf("[LEITOR %d] Email %d consumido do indice %d\n", id, email.id, indice_uso);

        printf("  Titulo: %s\n", email.titulo);
        printf("  Descricao: %s\n", email.descricao);
        printf("  Prioridade: %d\n", email.prioridade);

        // Libera os mutexes
        sem_post(&caixa->indices[indice_uso]);    // Libera indice para operacoes
    
        sleep(5);                                   // Tempo para sair
    }
    pthread_exit(0);

}

/* Calcula a nova prioridade de e-mails não consumidos*/
int calcular_prioridade(Email email){
    time_t tempo_atual = time(NULL);

    int tempo_espera = (int)difftime(tempo_atual, email.instante_entrada);
    int aumento = tempo_espera / INTERVALO_ENVELHECIMENTO;

    int prioridade_efetiva = email.prioridade + aumento;

    if (prioridade_efetiva > 3){
        prioridade_efetiva = 3;
    }

    return prioridade_efetiva;

}   