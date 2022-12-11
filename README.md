# raft-tfd


Projeto feito durante a UC de Tolerância de Faltas Distribuídas durante o 1º Semestre do MEI na FCUL.

Importante notar que para todo o desenvolvimento deste projeto foi escrito em JAVA e utilizada a framework gRPC.
Recomenda-se correr todo o projeto num ambiente que suporte esta framework.
Tal informação é salientada, devido a membros do grupo, terem experienciado problemas ao correr o projeto com computadores com processador M1 da Apple, sendo recomendado o ambiente Windows.

Este projeto consiste na implementação do algoritmo de consenso RAFT, estando este dividido em 3 projetos mais pequenos, cada um com o seu propósito.

ReplicaContract – Representa o contrato escrito na linguagem protobuf, onde estão definidas todas as operações e objetos que vão ser utilizados na comunicação entre máquinas.

Client – Como o nome indica, simboliza o que se aproxima de um cliente quanto à utilização deste algoritmo, permite submeter informação de forma automática e periódica ao sistema.

Replica – Todo o algoritmo de RAFT incluindo a comunicação entre réplicas, a replicação de informação e todos os detalhes intrínsecos ao algoritmo.

Para correr todo o projeto é necessário primeiramente instalar o contrato protobuf (ReplicaContract) sendo este normalmente instalado pelos membros do grupo através de IDEA (Intellij). Em seguida criar os .jars (caso desejado correr via terminal) tanto do cliente como das réplicas. Finalmente para correr basta seguir as instruções providenciadas por cada projeto, sendo necessário criar previamente um ficheiro com todos os IPs e portas (exemplo mais abaixo) que as réplicas irão assumir. Tal ficheiro também é requisitado pelo Cliente visto que o mesmo também precisa de tal informação para poder estabelecer a comunicação. 

Exemplo do ficheiro necessário:
```
localhost:5000
localhost:5001
localhost:5002
localhost:5003
localhost:5004
```
Para correr o projeto na linha de comandos, é necessário os seguintes procedimentos:

Para correr cada réplica:
Através do Maven, é obtido o JAR que será utilizado para correr o sistema. Após a obtenção do mesmo, que deverá ser idêntico a "Replica-1.0-jar-with-dependencies.jar" é apenas necessário, para cada réplica, executar o comando que se apresenta de seguida, no qual o id corresponde ao identificador da réplica (0 a 4), o segundo parâmetro é nada mais que a path do config file, que possui o endereços de cada réplica, e o último parâmetro é a path do ficheiro .txt onde serão armazenados os logs.

```
java -jar Replica-1.0-jar-with-dependencies.jar <id(>= 0)> <configFile(absolute path)> <logFile(absolute path)>
```

Para correr cada cliente:
A processo para a obtenção do jar é igual ao referido anteriormente para as réplicas. Posteriormente à obtenção do mesmo, é apenas necessário correr o comando apresentado em seguida, em que o primeiro e único argumento corresponde ao ficheiro config, referido anteriormente.

```
java -jar Client.jar <configFile(absolute path)>
```

(Atenção a eventuais linhas adicionais em branco ou outros caracteres não previstos, o ficheiro apenas deve conter algo semelhante ao que está exemplificado)
	
Toda a arquitetura foi testada no máximo com 5 réplicas sendo este o valor máximo pedido para este projeto.

Membros do grupo que participaram em todo o desenvolvimento do projeto:
Diogo Novo – 60400
João Arcanjo – 60405
João Lopes– 60493 
