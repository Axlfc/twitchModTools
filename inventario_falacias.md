# INVENTARIO FILOSÓFICO EXHAUSTIVO DE FALACIAS LÓGICAS
### Basado en Aristóteles · Hamblin · Walton · Woods · van Eemeren · Grootendorst · Kahneman · Tversky · Moore · Hume · Russell · Ryle · Dennett · Taleb · Johnson & Blair

---

> **Nota metodológica.** Este inventario distingue entre *paralogismo* (error no deliberado) y *sofisma* (manipulación intencional). Cada entrada incluye: nombre canónico, nombre latino o anglosajón cuando existe, descripción filosófica, estructura inferencial cuando aplica, y al menos un ejemplo. Las falacias están graduadas por **severidad** en tres niveles: ◆◆◆ (invalida completamente el argumento), ◆◆ (debilita sustancialmente el argumento), ◆ (debilita parcialmente o en contexto).

---

## PARTE I — FALACIAS FORMALES

*Errores en la estructura lógica pura. La conclusión no se sigue de las premisas aunque estas sean verdaderas.*

---

### BLOQUE 1 · Falacias silogísticas clásicas

**F001 · Afirmación del consecuente** *(Affirming the consequent)*
Severidad: ◆◆◆
Forma: Si P → Q; Q; ∴ P
El hecho de que la consecuencia sea verdadera no valida el antecedente. El condicional no es bicondicional.
Ejemplo: "Si hay infección, habrá fiebre. Hay fiebre. Luego hay infección." (la fiebre puede tener otras causas)

**F002 · Negación del antecedente** *(Denying the antecedent)*
Severidad: ◆◆◆
Forma: Si P → Q; ¬P; ∴ ¬Q
La falsedad del antecedente no implica la falsedad del consecuente.
Ejemplo: "Si estudias, aprobarás. No has estudiado. Luego, no aprobarás." (podrías aprobar de otras formas)

**F003 · Cuatro términos** *(Quaternio terminorum)*
Severidad: ◆◆◆
Forma: P1: Todo M es P; P2: Todo S es M' (donde M != M' por equivocación) ∴ Todo S es P
Un silogismo que aparenta tener tres términos pero usa cuatro porque un término cambia de significado entre premisas.
Ejemplo: "El fin justifica los medios. La muerte es el fin de la vida. Luego, la muerte justifica los medios." (equivocación sobre "fin")

**F004 · Indistribución del término medio**
Severidad: ◆◆◆
Forma: P1: Todo P es M; P2: Todo S es M ∴ Todo S es P (M no está distribuido)
El término medio no está distribuido en ninguna premisa, rompiendo la validez del silogismo.
Ejemplo: "Todos los perros son animales. Algunos animales son gatos. Luego, algunos perros son gatos." (el término medio "animales" no está distribuido)

**F005 · Ilícito mayor**
Severidad: ◆◆◆
Forma: P1: Todo M es P; P2: Ningún S es M ∴ Ningún S es P (P está distribuido en la conclusión pero no en la premisa)
El término mayor está distribuido en la conclusión pero no en la premisa mayor, tomando más extensión de la que se le había concedido.
Ejemplo: "Todos los humanos son mortales. Ningún ángel es humano. Luego, ningún ángel es mortal." (mortal queda distribuido indebidamente)

**F006 · Ilícito menor**
Severidad: ◆◆◆
Forma: P1: Todo P es M; P2: Todo M es S ∴ Todo S es P (S está distribuido en la conclusión pero no en la premisa)
El término menor está distribuido en la conclusión pero no en la premisa menor.
Ejemplo: 'Todos los gatos son mamíferos. Todos los mamíferos son animales. Luego, todos los animales son gatos.' (el término 'animales' se distribuye indebidamente)
**F007 · Dos premisas negativas**
Severidad: ◆◆◆
Forma: Ningún M es P; Ningún S es M ∴ (no se sigue conclusión válida)
De dos premisas negativas no puede extraerse ninguna conclusión positiva ni negativa válida.
Ejemplo: "Ningún pez es mamífero. Ningún reptil es pez. ∴ ?" (nada puede concluirse)

**F008 · Dos premisas particulares**
Severidad: ◆◆◆
Forma: Algún M es P; Algún S es M ∴ (no se sigue conclusión válida)
De dos premisas particulares no puede obtenerse una conclusión universal válida.
Ejemplo: 'Algunos políticos son honestos. Algunos honestos son pobres. Luego, algunos políticos son pobres.' (no se puede asegurar la conexión)
**F009 · Conclusión más fuerte que las premisas**
Severidad: ◆◆◆
Forma: Premisas particulares o contingentes ∴ Conclusión universal o necesaria
La conclusión es universal o necesaria cuando las premisas solo permiten una conclusión particular o contingente.
Ejemplo: 'Algún estudiante aprobó sin estudiar. Luego, todos aprobarán sin estudiar.'
**F010 · Silogismo disyuntivo incompleto**
Severidad: ◆◆
Forma: P ∨ Q; ¬P; ∴ Q — cuando la disyunción no es exhaustiva y existen otras alternativas no consideradas.
Ejemplo: 'O es lunes o es martes. No es lunes, luego es martes.' (cuando podría ser miércoles, si la disyunción no es exhaustiva)


---

### BLOQUE 2 · Falacias de cuantificadores

**F011 · Generalización existencial incorrecta**
Severidad: ◆◆◆
Forma: ∀x P(x) ∴ ∃x P(x) (presuponiendo falsamente que el conjunto no es vacío)
Inferir que algo existe en particular a partir de una verdad universal sin verificar la existencia del individuo.
Ejemplo: "Todos los unicornios tienen cuerno. Luego, existe al menos un unicornio con cuerno."

**F012 · Error de cuantificador intercambiado** *(Quantifier shift fallacy)*
Severidad: ◆◆◆
Forma: ∀x ∃y R(x,y) ∴ ∃y ∀x R(x,y)
Confundir "∀x ∃y R(x,y)" con "∃y ∀x R(x,y)".
Ejemplo: "Todos aman a alguien" ≠ "Hay alguien a quien todos aman."

**F013 · Generalización de casos vacíos** *(Vacuous truth confusion)*
Severidad: ◆◆
Forma: ∀x ∈ ∅: P(x) ∴ Q (tratar una verdad vacua como premisa con contenido)
Tratar una verdad vacua (verdadera porque el antecedente es imposible) como si tuviera contenido sustantivo.
Ejemplo: usar "Todos los unicornios son mansos" como premisa sustantiva en un argumento sobre mansedumbre.

---

### BLOQUE 3 · Falacias modales

**F014 · Error de modalidad básico** *(Modal fallacy)*
Severidad: ◆◆◆
Forma: ◇P ∴ □P
Confundir necesidad con posibilidad, o lo contingente con lo imposible.
Ejemplo: "Es posible que llueva. Luego, necesariamente lloverá en algún momento." (non sequitur modal)

**F015 · Necesitación errónea**
Severidad: ◆◆◆
Forma: Siempre P ∴ □P (confundir regularidad con necesidad lógica)
Inferir que algo es necesario porque siempre ha ocurrido: confusión entre necesidad lógica y regularidad empírica.
Ejemplo: "El sol siempre ha salido. Luego, es lógicamente necesario que salga mañana."

**F016 · Confusión de dicto / de re** *(De dicto / de re confusion)*
Severidad: ◆◆◆
Forma: □(∃x Px) ∴ ∃x(□Px)
Confundir la necesidad de una proposición (de dicto) con la necesidad de que una propiedad se aplique a un objeto (de re).
Ejemplo: "Necesariamente el número de planetas es > 0" (de dicto, analítica) ≠ "El número de planetas tiene necesariamente esa cantidad" (de re, falsa).

**F017 · Falacia de la posibilidad como probabilidad**
Severidad: ◆◆
Forma: ◇P ∴ P tiene alta probabilidad
Tratar "es posible que X" como si X tuviera probabilidad significativa o alta.
Ejemplo: "Es posible que este medicamento cause daño cerebral" usado para justificar su prohibición sin datos de frecuencia.

**F018 · Determinismo retrospectivo** *(Retrospective determinism / Hindsight necessity)*
Severidad: ◆◆
Forma: P ocurrió ∴ □P (confusión de necesidad fáctica con necesidad lógica)
Porque algo ocurrió, era inevitable que ocurriera. Confunde necesidad lógica con necesidad fáctica post-hoc.
Ejemplo: "La Primera Guerra Mundial tenía que estallar; las condiciones lo hacían inevitable." (confunde explicación con necesidad lógica)

**F019 · Falacia del hombre enmascarado** *(Masked man fallacy / Intensional fallacy)*
Severidad: ◆◆◆
Aplicar el principio de identidad de indiscernibles en contextos intensionales donde no aplica.
Forma: Sé que A es F. No sé que B es F. ∴ A ≠ B. (inválido si el contexto es intensional)
Ejemplo: "Sé quién es el alcalde. No sé quién es el asesino. Luego el alcalde no es el asesino."

---

### BLOQUE 4 · Otras falacias formales

**F020 · Non sequitur formal**
Severidad: ◆◆◆
Forma: P ∴ Q (sin vínculo lógico o regla de inferencia válida)
La conclusión no se sigue de las premisas por ninguna regla de inferencia reconocible.
Ejemplo: 'Tengo un coche rojo, luego mañana lloverá.'
**F021 · Afirmación de un disyunto en disyunción inclusiva**
Severidad: ◆◆
Forma: P ∨ Q; P ∴ ¬Q (en una disyunción que permite ambos disyuntos)
En una disyunción inclusiva (P ∨ Q), afirmar P no permite negar Q.
Ejemplo: "O estudia o trabaja. Estudia. Luego no trabaja." (puede hacer ambas)

**F022 · Falacia de la transitividad incorrecta**
Severidad: ◆◆◆
Forma: A R B; B R C ∴ A R C (donde la relación R no es transitiva)
Asumir que una relación es transitiva cuando no lo es.
Ejemplo: "Pedro es amigo de Juan. Juan es amigo de María. Luego, Pedro es amigo de María." (la amistad no es transitiva necesariamente)

---

## PARTE II — FALACIAS DE AMBIGÜEDAD
*(Fallaciae in dictione — aristotélicas)*

*Explotan imprecisiones semánticas, sintácticas o pragmáticas del lenguaje.*

---

**F023 · Equivocación** *(Equivocation)*
Severidad: ◆◆◆
Forma: P1(A es B); P2(B' es C) ∴ A es C (donde B y B' son la misma palabra con distinto significado)
Usar una misma palabra con dos significados distintos dentro del mismo argumento.
Ejemplo: "Las leyes de la naturaleza son leyes. Las leyes las hacen los hombres. Luego, los hombres hicieron las leyes de la naturaleza."

**F024 · Anfibología** *(Amphiboly)*
Severidad: ◆◆
Forma: Sentencia S tiene ambigüedad gramatical ∴ Interpretación errónea de S
Ambigüedad gramatical o sintáctica que hace que una premisa pueda interpretarse de formas contradictorias.
Ejemplo: "Vi al hombre con el telescopio." (¿quién tiene el telescopio?)

**F025 · Acento** *(Fallacia accentus)*
Severidad: ◆◆
Forma: Énfasis(S, palabra1) ∴ Significado1; Énfasis(S, palabra2) ∴ Significado2
Cambiar el significado de una proposición por el énfasis en una palabra diferente.
Ejemplo: "No deberías robar a tus AMIGOS." (implica que sí puedes robar a otros)

**F026 · Composición lógica** *(Fallacy of composition)*
Severidad: ◆◆◆
Forma: ∀x ∈ W: P(x) ∴ P(W)
Atribuir al todo las propiedades de sus partes.
Ejemplo: "Cada átomo de este muro es invisible. Luego, el muro es invisible."

**F027 · División lógica** *(Fallacy of division)*
Severidad: ◆◆◆
Forma: P(W) ∴ ∀x ∈ W: P(x)
Atribuir a las partes las propiedades del todo.
Ejemplo: "Esta empresa gana millones. Tu departamento forma parte de ella. Luego, tu departamento gana millones."

**F028 · Composición verbal aristotélica** *(Fallacia compositionis verbalis)*
Severidad: ◆◆
Forma: Ambigüedad por agrupar palabras que deben leerse por separado
Ambigüedad producida por juntar palabras que deben leerse separadas, distinta de la composición lógica.
Ejemplo clásico: "Puede caminar mientras está sentado" leído como "puede-caminar-mientras-está-sentado" (una capacidad) vs "puede / caminar mientras está sentado" (dos acciones).
Ejemplo: 'Puede caminar mientras está sentado' (leído como una sola capacidad paradójica)
**F029 · División verbal aristotélica** *(Fallacia divisionis verbalis)*
Severidad: ◆◆
Forma: Ambigüedad por separar palabras que deben leerse juntas
Ambigüedad producida por separar lo que debe leerse unido.
Ejemplo: 'Cinco es dos y tres' (interpretado como que el número 5 es a la vez el número 2 y el número 3 por separado)
**F030 · Figura de dicción** *(Fallacy of figure of speech)*
Severidad: ◆
Forma: Metáfora(M) ∴ Sentido_Literal(M)
Tratar expresiones metafóricas, figuradas o idiomáticas como si fueran literales, o viceversa.
Ejemplo: Tomar "estoy muerto de hambre" como afirmación médica.

**F031 · Reificación** *(Hypostatization / Reification)*
Severidad: ◆◆
Forma: Abstracción A ∴ Entidad concreta con capacidad causal
Tratar una abstracción como si fuera una entidad concreta con agencia causal.
Ejemplo: "La historia nos enseña que los imperios caen." (la historia no es un agente)

**F032 · Equivocación de proceso/producto** *(Process-product ambiguity)*
Severidad: ◆◆
Forma: Confundir las propiedades del acto de creación con las del objeto creado
Confundir el proceso de hacer algo con el producto resultante.
Ejemplo: "La construcción del puente duró dos años" (proceso) vs "La construcción es sólida" (producto).

**F033 · Confusión uso/mención** *(Use-mention confusion)*
Severidad: ◆◆◆
Forma: Confundir el hablar de una palabra (mención) con el uso de su referente
Usar una palabra y hablar sobre esa palabra como si fueran la misma cosa.
Ejemplo: "Gato tiene cuatro letras." (correcto) confundido con "Gato tiene cuatro letras, luego los gatos tienen cuatro letras."

**F034 · Falacia etimológica** *(Etymological fallacy)*
Severidad: ◆◆
Forma: Etimología(X) = Significado_Actual(X)
Argumentar que el significado actual de una palabra debe ser el original o el "verdadero".
Ejemplo: "Política viene de polis, luego la política solo puede referirse a lo comunitario-local."

**F035 · Definición persuasiva** *(Persuasive definition — Stevenson)*
Severidad: ◆◆
Forma: Definición(X) = X + carga emocional/valorativa para sesgar
Redefinir un término cargado de valor para capturar su connotación en beneficio propio.
Ejemplo: "La verdadera libertad es la obediencia voluntaria a la ley divina."

**F036 · Definición circular**
Severidad: ◆◆◆
Forma: X se define mediante Y; Y se define mediante X
Definir un término usando el mismo término, explícita o implícitamente.
Ejemplo: "El opio duerme porque tiene virtud dormitiva." (Molière)

**F037 · Definición demasiado amplia** *(Overextension)*
Severidad: ◆◆
Forma: Definición(X) incluye casos que no pertenecen a la categoría X
La definición incluye casos que claramente no pertenecen al concepto.
Ejemplo: 'Un ave es un animal que pone huevos.' (incluye a los ornitorrincos)
**F038 · Definición demasiado estrecha** *(Underextension)*
Severidad: ◆◆
Forma: Definición(X) excluye casos que sí pertenecen a la categoría X
La definición excluye casos que claramente sí pertenecen al concepto.
Ejemplo: 'Un mamífero es un animal con cuatro patas.' (excluye a los humanos)
**F039 · Lenguaje cargado emocionalmente** *(Loaded language / Emotive language)*
Severidad: ◆◆
Forma: Uso de lenguaje con fuerte carga E para evitar la argumentación sobre hechos H
Usar términos con fuerte carga emotiva o connotativa para sesgar la percepción sin añadir contenido proposicional.
Ejemplo: Llamar "asesinato" a un procedimiento médico legal, o "eliminación de tejido" a un homicidio.

**F040 · Eufemismo encubridor** *(Euphemistic fallacy)*
Severidad: ◆◆
Forma: Sustitución de término X por eufemismo Y para suavizar una realidad negativa
Usar eufemismos para suavizar una realidad de manera que cambia la evaluación moral o epistémica del argumento.
Ejemplo: "Daños colaterales" en lugar de "civiles muertos."

---

## PARTE III — FALACIAS DE RELEVANCIA
*(Irrelevantia — las premisas no son pertinentes para la conclusión)*

---

### BLOQUE 5 · Ad hominem y variantes

**F041 · Ad hominem abusivo** *(Abusive ad hominem)*
Severidad: ◆◆◆
Forma: P afirma X; P tiene rasgos negativos ∴ X es falso
Atacar el carácter, apariencia o vida personal del interlocutor en lugar de su argumento.
Ejemplo: "No puedes hablar de economía; eres un fracasado financiero."

**F042 · Ad hominem circunstancial**
Severidad: ◆◆
Forma: P afirma X; P tiene intereses en que X sea cierto ∴ X es falso
Señalar que el interlocutor tiene intereses personales que supuestamente invalidan su posición, sin refutar el argumento.
Ejemplo: "Claro que defiendes las vacunas; te paga la industria farmacéutica."

**F043 · Tu quoque** *(Tu quoque — tú también)*
Severidad: ◆◆◆
Forma: P critica Y; P comete Y ∴ La crítica de P es inválida
Responder a una crítica señalando que el crítico hace lo mismo, como si eso invalidara la crítica.
Ejemplo: "Me dices que no debo fumar, pero tú también fumabas."

**F044 · Bulverismo** *(Bulverism — C.S. Lewis)*
Severidad: ◆◆
Forma: Explicar el origen psicológico de la creencia de P ∴ Refutación de dicha creencia
Explicar por qué la persona cree lo que cree (motivos psicológicos, sociológicos) como si eso invalidara su argumento, sin refutarlo directamente.
Ejemplo: "Solo defiendes el feminismo porque tu madre era feminista; eso explica tu sesgo."

**F045 · Envenenamiento del pozo** *(Poisoning the well)*
Severidad: ◆◆◆
Forma: Presentar información negativa de P antes de su intervención ∴ Argumento de P invalidado
Presentar información desacreditadora sobre el interlocutor antes de que pueda argumentar, de modo que cualquier cosa que diga quede contaminada.
Ejemplo: "Antes de que hable mi oponente, deben saber que ha sido acusado de corrupción."

**F046 · Falacia genética** *(Genetic fallacy)*
Severidad: ◆◆◆
Forma: La idea I proviene de un origen O desacreditado ∴ I es falsa
Juzgar el valor de una idea por su origen (histórico, social, psicológico), no por su contenido.
Ejemplo: "Esa teoría surgió en el contexto del nazismo, luego es falsa."

---

### BLOQUE 6 · Apelaciones improcedentes

**F047 · Ad baculum** *(Apelación a la fuerza / la vara)*
Severidad: ◆◆◆
Forma: Si no aceptas X, ocurrirá un daño inducido D ∴ X es aceptable
Usar la amenaza, la coerción o el poder como argumento para aceptar una conclusión.
Ejemplo: "Aceptarás mi propuesta si sabes lo que te conviene."

**F048 · Ad misericordiam** *(Apelación a la piedad)*
Severidad: ◆◆
Forma: Apelar a la piedad de P para que acepte la conclusión C sin pruebas
Apelar a la lástima o compasión como sustituto de la evidencia.
Ejemplo: "Deberías darme el trabajo; tengo tres hijos y estoy desesperado."

**F049 · Ad populum** *(Apelación a la mayoría / Argumentum ad numerum)*
Severidad: ◆◆◆
Forma: La mayoría cree X ∴ X es verdadero
Concluir que algo es verdad porque mucha gente lo cree.
Ejemplo: "Millones de personas creen en la homeopatía, algo de verdad tendrá."

**F050 · Efecto de arrastre** *(Bandwagon fallacy)*
Severidad: ◆◆
Forma: X es tendencia o popular ∴ Debes aceptar o unirte a X
Variante del ad populum: unirse a la mayoría o a la tendencia como justificación de una decisión o creencia.
Ejemplo: 'Todo el mundo está comprando criptomonedas, así que deben ser una inversión segura.'
**F051 · Ad verecundiam — fuera de dominio** *(Appeal to authority — irrelevant expertise)*
Severidad: ◆◆◆
Forma: Autoridad A dice X (siendo X ajeno al dominio experto de A) ∴ X es cierto
Invocar la autoridad de alguien como sustituto de evidencia, especialmente fuera de su área de competencia.
Ejemplo: "Este famoso físico dice que la acupuntura funciona, luego funciona."

**F052 · Apelación a la autoridad anónima**
Severidad: ◆◆◆
Forma: Referencia a fuentes expertas sin identificar ∴ X es cierto
"Estudios demuestran…", "expertos dicen…" sin identificar la fuente ni sus credenciales.
Ejemplo: 'Científicos de prestigio aseguran que este producto rejuvenece.'
**F053 · Apelación a la autoridad por consenso fabricado**
Severidad: ◆◆◆
Forma: Presentar una opinión popular o mayoritaria como si fuera un consenso científico
Presentar como consenso experto lo que es solo opinión mayoritaria, sectaria o parcial.
Ejemplo: 'La mayoría de la gente prefiere este sistema, por lo tanto es el mejor técnicamente.'
**F054 · Ad ignorantiam** *(Apelación a la ignorancia)*
Severidad: ◆◆◆
Forma: No se ha probado que X sea falso ∴ X es verdadero
Concluir que algo es verdad porque no se ha probado que sea falso, o viceversa.
Ejemplo: "Nadie ha demostrado que los fantasmas no existen, luego existen."

**F055 · Ad antiquitatem** *(Apelación a la tradición)*
Severidad: ◆◆
Forma: Tradicionalmente se ha hecho X ∴ X es correcto o bueno
Argumentar que algo es correcto porque siempre se ha hecho así.
Ejemplo: "El matrimonio siempre ha sido entre hombre y mujer, luego así debe ser."

**F056 · Ad novitatem** *(Apelación a la novedad)*
Severidad: ◆◆
Forma: X es novedoso ∴ X es superior a lo anterior
Lo nuevo es mejor por el mero hecho de ser nuevo.
Ejemplo: "Esta terapia es revolucionaria y reciente, debe ser superior a las antiguas."

**F057 · Ad naturam** *(Apelación a la naturaleza / Naturalismo informal)*
Severidad: ◆◆
Forma: X es natural ∴ X es intrínsecamente bueno
Lo natural es bueno; lo artificial, malo, sin justificación ulterior.
Ejemplo: "Este veneno es completamente natural, luego es seguro."

**F058 · Apelación al miedo** *(Ad metum)*
Severidad: ◆◆◆
Forma: Evocar temor sobre las consecuencias de X para forzar su rechazo
No es una amenaza directa (ad baculum), sino la evocación del miedo como argumento para aceptar una conclusión.
Ejemplo: "Si no aprobamos esta ley, los terroristas acabarán con nuestra civilización."

**F059 · Apelación al asco / repugnancia moral** *(Yuck factor / Argument from disgust)*
Severidad: ◆◆
Forma: X provoca repulsa emocional ∴ X es moralmente condenable
Usar la respuesta emocional de repulsa como sustituto de argumento ético (Haidt).
Ejemplo: "Eso simplemente me parece asqueroso, luego no debe permitirse."

**F060 · Apelación a la riqueza** *(Ad crumenam)*
Severidad: ◆◆
Forma: P es rico ∴ Los juicios de P son más válidos
Lo que dice el rico es más válido o verdadero por el hecho de ser rico.
Ejemplo: 'Es un empresario de éxito, así que su visión sobre la educación debe ser correcta.'
**F061 · Apelación a la pobreza** *(Ad lazarum)*
Severidad: ◆◆
Forma: P es pobre ∴ Los juicios de P son más auténticos o verdaderos
Lo que dice el pobre es más auténtico o verdadero por el hecho de serlo.
Ejemplo: 'Este monje vive en la miseria, su palabra es la verdad pura.'
**F062 · Apelación a la adulación** *(Ad captandum vulgus)*
Severidad: ◆◆
Forma: Halagar al auditorio o a P ∴ Ganar adhesión a la conclusión C
Ganar adhesión mediante halagos al auditorio en lugar de argumentos.
Ejemplo: "Ustedes, que son personas tan inteligentes, sabrán reconocer que mi propuesta es la correcta."

**F063 · Apelación al odio** *(Ad odium)*
Severidad: ◆◆◆
Forma: Inducir odio hacia el proponente o su grupo ∴ Refutar su posición
Inducir hostilidad hacia el oponente o hacia su posición como sustituto de argumento.
Ejemplo: 'Tus enemigos apoyan esta ley, ¿vas a estar de su lado?'
**F064 · Apelación al ridículo** *(Reductio ad ridiculum / Appeal to ridicule)*
Severidad: ◆◆◆
Forma: Presentar X como ridículo sin análisis ∴ X es falso
Presentar la posición contraria como absurda o ridícula sin demostrar por qué lo es. Distinto de la reductio ad absurdum legítima.
Ejemplo: "¿De verdad quieres defender esa idea tan ridícula?"

**F065 · Apelación al misterio** *(Mysterian fallacy)*
Severidad: ◆◆
Forma: X es incomprensible ∴ X tiene una explicación sobrenatural
Usar la incomprensibilidad o el misterio como argumento positivo a favor de una posición sobrenatural o metafísica.
Ejemplo: "La conciencia es tan misteriosa que solo puede explicarse por el alma."

**F066 · Argumentum ex silentio** *(Argumento del silencio)*
Severidad: ◆◆
Forma: Inexistencia de mención o registro de X ∴ X no sucedió
Concluir que algo es verdad a partir de la ausencia de testimonio en contra, o que algo no ocurrió porque no hay registro de ello.
Ejemplo: 'Marco Polo no mencionó la Gran Muralla, por lo tanto no existía en su época.'


---

### BLOQUE 7 · Distorsión del argumento ajeno

**F067 · Hombre de paja** *(Straw man)*
Severidad: ◆◆◆
Forma: Atacar una versión simplificada o deformada de X ∴ X queda refutado
Distorsionar, exagerar o simplificar la posición del oponente para refutarla más fácilmente.
Ejemplo: "Quieres reducir el presupuesto militar, ¡así que quieres que nos invadan!"

**F068 · Motte and bailey**
Severidad: ◆◆◆
Forma: Alternar entre una posición fácil de defender (motte) y una controvertida (bailey)
Se defiende una posición controvertida (bailey) pero cuando se ataca, se retira a una posición obvia e indefendible (motte), luego se regresa al bailey sin que se haya resuelto el debate sobre este.
Ejemplo: Defender "el patriarcado controla todas las instituciones" (bailey), pero cuando se critica, retirarse a "existen desigualdades de género" (motte), y luego regresar al bailey como si se hubiera validado.

**F069 · Red herring** *(Arenque rojo / Pista falsa)*
Severidad: ◆◆◆
Forma: Introducir un tema B ajeno al debate para distraer del tema central A
Introducir un tema irrelevante para desviar la atención del argumento central.
Ejemplo: '¿Por qué no bajamos los impuestos? Bueno, lo importante es hablar de la unidad del país.'
**F070 · Ignoratio elenchi** *(Conclusión irrelevante)*
Severidad: ◆◆◆
Forma: Probar una tesis P1 para concluir una tesis P2 no relacionada
Probar algo diferente de lo que se discute, aunque la prueba en sí sea válida.
Categoría madre que engloba el hombre de paja, el red herring y otras falacias de relevancia.
Ejemplo: 'Este sospechoso es una mala persona (probado), luego cometió este asesinato (no se sigue).'
**F071 · Decontextualización** *(Quoting out of context)*
Severidad: ◆◆◆
Forma: Extraer fragmentos de un discurso para alterar su sentido original
Extraer afirmaciones de su contexto argumentativo y presentarlas como si dijeran algo diferente.
Ejemplo: 'El autor escribió "no me gusta la guerra", pero si citamos solo "me gusta la guerra" cambiamos su mensaje.'
**F072 · Lógica de la olla** *(Kettle logic — Freud)*
Severidad: ◆◆
Forma: Ofrecer múltiples defensas que se contradicen entre sí
Ofrecer simultáneamente múltiples argumentos contradictorios que no pueden ser todos verdaderos.
Ejemplo de Freud: "No le rompí la olla / La olla ya estaba rota / Nunca le devolví ninguna olla."
Ejemplo: 'No te presté el libro, además te lo devolví ayer y ya estaba roto cuando me lo diste.'
**F073 · Kafkatrapping**
Severidad: ◆◆◆
Forma: Cualquier negación de la acusación X ∴ Prueba de culpabilidad de X
Construcción donde cualquier negación de la acusación se toma como prueba adicional de culpabilidad.
Ejemplo: "Si niegas ser racista, eso demuestra que no eres consciente de tu propio racismo."

---

## PARTE IV — FALACIAS DE PRESUNCIÓN
*(Asumen como verdadero algo que debería demostrarse)*

---

### BLOQUE 8 · Circularidad y preguntas cargadas

**F074 · Petición de principio** *(Petitio principii / Begging the question)*
Severidad: ◆◆◆
Forma: La conclusión C se asume como verdadera en las premisas de su propia prueba
La conclusión está implícita en las premisas; el argumento es circular.
Ejemplo: "La Biblia es verdadera porque lo dice Dios, y sabemos que Dios existe porque lo dice la Biblia."

**F075 · Pregunta compleja** *(Complex question / Plurium interrogationum)*
Severidad: ◆◆◆
Forma: Realizar una pregunta que contiene una presupuesto no aceptada
Formular una pregunta que presupone algo no establecido.
Ejemplo: "¿Cuándo dejaste de mentir?" (presupone que mentiste)

**F076 · Falsa dicotomía** *(False dilemma / Either-or fallacy)*
Severidad: ◆◆◆
Forma: Reducir las opciones a dos cuando hay más alternativas disponibles
Presentar solo dos opciones cuando existen más.
Ejemplo: "O estás con nosotros o estás contra nosotros."

**F077 · Punto medio falso** *(Argumentum ad temperantiam / Middle ground fallacy)*
Severidad: ◆◆◆
Forma: Asumir que la verdad es el promedio exacto entre dos posiciones extremas
Asumir que la posición correcta siempre está entre dos extremos. La verdad no es necesariamente el promedio de las posiciones en debate.
Ejemplo: "Algunos dicen que la Tierra tiene 4.500 millones de años; otros dicen 6.000 años. La verdad estará en el medio." (falso: el consenso científico no es un punto medio)

**F078 · Carga de la prueba desplazada** *(Burden of proof shift)*
Severidad: ◆◆◆
Forma: Quien afirma X no aporta pruebas y exige que el otro pruebe ¬X
Exigir al oponente que pruebe la falsedad de lo que uno afirma sin haber ofrecido evidencia propia.
Ejemplo: "Demuéstrame que Dios no existe."

---

### BLOQUE 9 · Pendiente resbaladiza y causalidad espuria

**F079 · Pendiente resbaladiza conceptual** *(Slippery slope — conceptual)*
Severidad: ◆◆
Forma: Aceptar un cambio pequeño A llevará inevitablemente a un extremo Z
Afirmar que aceptar una posición llevará inevitablemente a aceptar posiciones cada vez más extremas, sin justificar los pasos intermedios.
Ejemplo: "Si permitimos el matrimonio homosexual, pronto se permitirá casarse con animales."

**F080 · Pendiente resbaladiza causal**
Severidad: ◆◆
Forma: A → B → C (sin demostrar el vínculo causal real entre los pasos)
Afirmar que una acción causará inevitablemente una cadena de consecuencias negativas sin justificar los mecanismos causales.
Ejemplo: "Si legalizamos el cannabis, en diez años habremos destruido a toda una generación."

**F081 · Post hoc ergo propter hoc** *(Correlación temporal como causalidad)*
Severidad: ◆◆◆
Forma: Evento A ocurrió antes que B ∴ A es la causa de B
Concluir que A causó B porque A precedió a B.
Ejemplo: "Tomé vitamina C y se me curó el resfriado en una semana. La vitamina C cura los resfriados."

**F082 · Cum hoc ergo propter hoc** *(Correlación simultánea como causalidad)*
Severidad: ◆◆◆
Forma: Evento A y B ocurren al mismo tiempo ∴ A causa B
Concluir causalidad a partir de correlación simultánea.
Ejemplo: "Los países con más chocolateros tienen más premios Nobel. El chocolate aumenta el rendimiento cognitivo."

**F083 · Inversión de causa y efecto** *(Reverse causation)*
Severidad: ◆◆◆
Forma: A causa B ∴ B causa A (invertir la dirección de la causalidad)
Confundir qué fenómeno causa cuál.
Ejemplo: "Los hospitales enferman a la gente" (porque en los hospitales hay enfermos).

**F084 · Causa común ignorada** *(Confounding variable / Common cause fallacy)*
Severidad: ◆◆◆
Forma: A y B están correlacionados ∴ A causa B (ignorando una causa común C)
Atribuir causalidad entre A y B ignorando que una tercera variable C causa ambas.
Ejemplo: "Los países con más televisores tienen más esperanza de vida. Comprar televisores alarga la vida." (la variable de confusión es el nivel de desarrollo)

**F085 · Sobredeterminación causal ignorada**
Severidad: ◆◆
Forma: Atribuir el efecto a una sola causa cuando existen múltiples causas independientes
Asumir que solo puede haber una causa cuando pueden coexistir varias suficientes.
Ejemplo: 'El incendio se debió a un cortocircuito.' (ignorando que hubo también una fuga de gas)
**F086 · Agente único** *(Single cause fallacy)*
Severidad: ◆◆
Forma: Simplificar un fenómeno multicausal atribuyéndolo a un único agente
Atribuir a una sola causa o agente un fenómeno que es resultado de múltiples factores.
Ejemplo: "La Segunda Guerra Mundial la causó Hitler."

**F087 · Non causa pro causa** *(Falsa causa — aristotélica)*
Severidad: ◆◆◆
Forma: Presentar como causa algo que no tiene relación causal real
Presentar como causa de algo lo que no lo es. Categoría madre que incluye el post hoc y el cum hoc.
Ejemplo: 'La caída de la bolsa fue causada por el mal clima de esta mañana.'


---

### BLOQUE 10 · Generalizaciones y muestras defectuosas

**F088 · Generalización apresurada** *(Hasty generalization / Secundum quid)*
Severidad: ◆◆◆
Forma: Extraer una conclusión general a partir de una muestra insuficiente
Extraer una regla general a partir de un número insuficiente de casos.
Ejemplo: "Conozco dos alemanes poco amistosos. Los alemanes son fríos."

**F089 · Accidente** *(Fallacy of accident)*
Severidad: ◆◆◆
Forma: Aplicar una regla general a un caso cuyas circunstancias lo hacen excepcional
Aplicar una regla general a un caso particular donde claramente no corresponde.
Ejemplo: "Devolver lo prestado es correcto. Luego debes devolverle el cuchillo a tu amigo aunque quiera hacer daño con él."

**F090 · Accidente inverso** *(Converse accident / Secundum quid inverso)*
Severidad: ◆◆◆
Forma: Usar un caso excepcional para fundamentar una regla general
Construir una regla general a partir de un caso excepcional.
Ejemplo: "En legítima defensa se puede matar. Luego, matar puede ser moralmente aceptable en general."

**F091 · Sesgo de selección** *(Selection bias)*
Severidad: ◆◆◆
Forma: Usar una muestra sesgada para representar a toda la población
Usar una muestra no representativa del universo relevante para apoyar una conclusión general.
Ejemplo: 'Encuestamos a personas en un club de golf y todos son ricos, luego el país es rico.'
**F092 · Generalización desde casos típicos**
Severidad: ◆◆
Forma: Generalizar las propiedades del miembro más típico a toda la categoría
Tomar el caso prototípico de una categoría como representativo de toda ella, ignorando la varianza real.
Ejemplo: 'Un ave típica vuela, luego todas las aves vuelan.' (ignora pingüinos)
**F093 · Argumento desde la incredulidad personal** *(Argument from personal incredulity)*
Severidad: ◆◆◆
Forma: No ser capaz de imaginar cómo X es posible ∴ X es falso
"No puedo imaginar cómo X podría ser verdad (o ser resultado de Y), luego X es falso (o Y no lo causó)."
Ejemplo: "No puedo imaginar cómo la complejidad del ojo pudo surgir por selección natural, luego fue diseñado."

**F094 · Argumento desde la complejidad** *(Argument from complexity)*
Severidad: ◆◆
Forma: La complejidad de X ∴ X debe haber sido diseñado por una inteligencia
Porque algo es complejo, no puede ser resultado de procesos no dirigidos o no intencionales.
Variante del anterior, frecuente en argumentos de diseño inteligente.
Ejemplo: 'El ojo humano es tan complejo que no puede ser fruto de la evolución, requiere un diseñador.'


---

### BLOQUE 11 · Falacias de definición y clasificación

**F095 · No verdadero escocés** *(No true Scotsman)*
Severidad: ◆◆◆
Forma: Rechazar un contraejemplo a una generalización cambiando la definición del grupo
Modificar ad hoc la definición de un grupo para excluir contraejemplos embarazosos.
Ejemplo: "Ningún verdadero escocés pondría azúcar en la avena." "Pero mi tío escocés lo hace." "Entonces no es un verdadero escocés."

**F096 · Falacia del umbral / sorites** *(Sorites fallacy / Line-drawing fallacy)*
Severidad: ◆◆
Forma: La falta de una frontera nítida entre A y B ∴ No existe diferencia entre A y B
Argumentar que, dado que no hay una línea precisa entre dos extremos, la distinción no existe.
Ejemplo: "No puedes decir en qué momento alguien se queda calvo, luego la calvicie no existe."

**F097 · Error de categoría** *(Category mistake — Ryle)*
Severidad: ◆◆◆
Forma: Asignar a una entidad propiedades de una categoría lógica o física distinta
Atribuir a una entidad propiedades que pertenecen a una categoría ontológica distinta.
Ejemplo: "¿Cuánto pesa la Universidad de Oxford?" / "¿Dónde está exactamente la mente?"

**F098 · Falacia de la esencia** *(Essentialism fallacy)*
Severidad: ◆◆
Forma: Asumir que un grupo tiene una esencia inmutable que define a todos sus miembros
Asumir que todos los miembros de una categoría comparten una esencia fija y homogénea.
Ejemplo: Razonar sobre "los migrantes" como si todos compartieran exactamente las mismas propiedades.

---

## PARTE V — FALACIAS DE ANALOGÍA
*(Sección ausente en la mayoría de inventarios; una de las omisiones más graves)*

---

**F099 · Falsa analogía** *(False analogy / Weak analogy)*
Severidad: ◆◆◆
Forma: Comparar dos objetos A y B basándose en similitudes irrelevantes para la conclusión
Comparar dos casos cuyas diferencias relevantes invalidan la inferencia.
Ejemplo: "El cerebro es como un ordenador, luego puede 'reprogramarse' conductualmente con la misma facilidad."

**F100 · Analogía por semejanza superficial**
Severidad: ◆◆◆
Forma: A y B son similares en un rasgo superficial ∴ A y B son equivalentes en todo
Similitud en propiedades accidentales, no en la relación estructural relevante al argumento.
Ejemplo: "Los nazis usaban uniformes. Los policías usan uniformes. Luego, los policías son nazis."

**F101 · Analogía extendida indebidamente** *(Slippery analogy)*
Severidad: ◆◆
Forma: Llevar una analogía más allá del punto donde las similitudes se mantienen
Una analogía válida en un dominio se extiende más allá de sus límites sin justificación.
Ejemplo: "Si los contratos privados se pueden anular cuando hay abuso de poder, también las leyes estatales deben anularse cuando el Estado abusa."

**F102 · Falacia de la metáfora cosificada** *(Metaphor literalized)*
Severidad: ◆◆
Forma: Extraer consecuencias literales a partir de una comparación metafórica
Tratar una metáfora explicativa como si fuera una equivalencia real y extraer consecuencias de ella.
Ejemplo: "La economía es un organismo vivo, luego tiene ciclos de enfermedad que no debemos interrumpir artificialmente."

**F103 · Falacia de la analogía legal defectuosa**
Severidad: ◆◆◆
Forma: Aplicar un precedente legal a un caso que no comparte la misma ratio decidendi
Aplicar precedente jurídico a un caso sin verificar que las propiedades jurídicamente relevantes sean las mismas.
Ejemplo: 'Si el robo de pan por necesidad extrema se perdona, el robo de un reloj de lujo también debe perdonarse.'
**F104 · Falacia de la analogía moral defectuosa**
Severidad: ◆◆◆
Forma: Comparar dos actos morales ignorando diferencias éticas fundamentales
Comparar situaciones morales sin verificar que las propiedades moralmente relevantes sean equivalentes.
Ejemplo: "Matar en guerra es igual que el aborto; ambos son quitar una vida." (ignora diferencias moralmente relevantes)

**F105 · Falacia de la analogía por resultado** *(Results analogy)*
Severidad: ◆◆
Forma: Dos acciones con resultados parecidos se juzgan como moralmente idénticas
Dos acciones producen resultados similares; luego son equivalentes en todos los aspectos relevantes.
Ejemplo: 'Ambos causaron una muerte, luego son igual de culpables.' (sin distinguir accidente de intención)


---

## PARTE VI — FALACIAS CAUSALES Y ABDUCTIVAS
*(La inferencia a la mejor explicación y sus patologías)*

---

**F106 · Falacia de la única explicación** *(Single explanation fallacy)*
Severidad: ◆◆◆
Forma: Asumir que la primera explicación satisfactoria encontrada es la única válida
Concluir que la única explicación posible considerada es la verdadera, sin explorar alternativas.
Ejemplo: "No puede haber otra explicación; claramente fue el mayordomo."

**F107 · Navaja de Ockham mal aplicada** *(Ockham's razor misapplication)*
Severidad: ◆◆
Forma: Elegir la hipótesis más simple ignorando la evidencia que requiere una más compleja
Aplicar parsimonia explicativa de forma mecánica: confundir la hipótesis más simple con la más verdadera, ignorando que "más simple" es dependiente del marco teórico.
Ejemplo: 'Es más fácil creer que la Tierra es plana que entender la física de la gravedad, por tanto es plana.'
**F108 · Rescate ad hoc** *(Ad hoc rescue / Epicycle fallacy)*
Severidad: ◆◆◆
Forma: Añadir supuestos incomprobables a una teoría para evitar que sea refutada
Modificar una teoría con hipótesis auxiliares improvisadas para salvarla de la refutación, sin que las hipótesis añadidas sean independientemente comprobables.
Ejemplo: "Mi teoría predijo X, pero ocurrió Y. Eso se explica por el factor Z." (Z inventado a posteriori)

**F109 · Confirmación circular abductiva**
Severidad: ◆◆◆
Forma: La hipótesis se justifica por unos datos que solo se explican por la propia hipótesis
La hipótesis explica los datos, y los datos confirman la hipótesis, sin que haya predicciones independientes que puedan falsarla.
Ejemplo: 'Este hombre tiene poderes porque mueve objetos, y sabemos que mueve objetos por sus poderes.'
**F110 · Pensamiento mágico como abducción**
Severidad: ◆◆◆
Forma: Creer que existe una conexión causal física entre un símbolo y un evento
Atribuir causalidad a conexiones simbólicas, rituales o supersticiosas sin mecanismo causal verificable.
Ejemplo: "Rompí un espejo y luego tuve mala suerte; el espejo la causó."

**F111 · Falacia de la irreproducibilidad como confirmación**
Severidad: ◆◆◆
Forma: Argumentar que una teoría es cierta porque nadie ha logrado replicar el experimento que la niega
"Nadie ha podido replicar el experimento que refuta mi teoría" usado como si esto confirmara la teoría.
Ejemplo: 'Nadie ha podido repetir ese estudio que decía que mi producto era tóxico, así que es seguro.'
**F112 · Abducción desde la ignorancia** *(Inference to the only explanation)*
Severidad: ◆◆◆
Forma: Concluir que una explicación es verdadera solo porque no se conocen alternativas
Inferir que X es verdad porque no se conoce ninguna otra explicación, ignorando que la ignorancia no es evidencia positiva.
Ejemplo: 'No sabemos cómo se construyeron las pirámides, así que fueron los extraterrestres.'


---

## PARTE VII — FALACIAS INDUCTIVAS Y ESTADÍSTICAS

---

### BLOQUE 12 · Sesgos de muestra y selección de datos

**F113 · Falacia del francotirador de Texas** *(Texas sharpshooter fallacy)*
Severidad: ◆◆◆
Forma: Encontrar patrones en datos aleatorios y crear una hipótesis a posteriori
Seleccionar a posteriori los datos que confirman la hipótesis, ignorando el resto; como disparar al azar y luego pintar la diana sobre los agujeros.
Ejemplo: 'Miré los números premiados y todos tenían un 7, ¡hay una conspiración del 7!'
**F114 · Cherry-picking** *(Suppressed evidence / Selective evidence)*
Severidad: ◆◆◆
Forma: Elegir solo los datos que apoyan la tesis y descartar deliberadamente el resto
Seleccionar solo la evidencia favorable e ignorar sistemáticamente la contraria.
Distinto del anterior: aquí la selección es deliberada y no necesariamente a posteriori.
Ejemplo: 'Este estudio dice que el azúcar es sano (financiado por la industria), ignoremos los otros cien estudios.'
**F115 · Sesgo de confirmación como falacia argumentativa**
Severidad: ◆◆◆
Forma: Presentar únicamente la evidencia confirmatoria ignorando la disonante
Presentar solo evidencia confirmatoria mientras se ignora o descarta la disconfirmatoria como si fuera irrelevante.
Ejemplo: 'Para demostrar que mi gestión es buena, solo mostraré los meses en los que el paro bajó.'
**F116 · Falacia del superviviente** *(Survivorship bias)*
Severidad: ◆◆◆
Forma: Analizar solo los casos que 'sobrevivieron' a un filtro, ignorando los fallidos
Razonar solo a partir de los casos exitosos (supervivientes), ignorando los que no lo fueron y que no están disponibles para análisis.
Ejemplo: "Los emprendedores exitosos no tienen estudios. Los estudios no sirven para emprender." (ignorando todos los fracasados sin estudios)

**F117 · Falacia del anecdotario** *(Anecdotal evidence)*
Severidad: ◆◆◆
Forma: Validar una regla general basándose en una sola experiencia personal
Usar experiencias personales o casos aislados en lugar de evidencia sistemática y representativa.
Ejemplo: 'Mi abuelo fumó siempre y vivió 100 años, por tanto el tabaco no es perjudicial.'
**F118 · Ecológica** *(Ecological fallacy)*
Severidad: ◆◆◆
Forma: Atribuir a un individuo una propiedad basada solo en estadísticas del grupo
Inferir propiedades individuales a partir de datos agregados de un grupo.
Ejemplo: "En ese barrio la renta media es alta, luego ese vecino en particular debe ser rico."

---

### BLOQUE 13 · Falacias probabilísticas y bayesianas

**F119 · Falacia del jugador** *(Gambler's fallacy / Monte Carlo fallacy)*
Severidad: ◆◆◆
Forma: Creer que la probabilidad de un suceso independiente cambia según los resultados previos
Creer que eventos aleatorios pasados afectan la probabilidad de eventos futuros independientes.
Ejemplo: "Ha salido cara diez veces seguidas, ahora seguro que sale cruz."

**F120 · Falacia del jugador inversa** *(Hot hand fallacy)*
Severidad: ◆◆
Forma: Asumir que una racha de suerte continuará de forma necesaria
Creer que una racha exitosa pasada continuará en el futuro en contextos donde los eventos son independientes.
Ejemplo: 'Ha metido tres canastas seguidas, la siguiente la mete con total seguridad.'
**F121 · Ignorancia de la tasa base** *(Base rate neglect — Kahneman & Tversky)*
Severidad: ◆◆◆
Forma: Evaluar la probabilidad de un evento ignorando su frecuencia previa en la población
Ignorar la frecuencia base de un fenómeno al evaluar probabilidades condicionales.
Ejemplo: Dar positivo en un test con 5% de falsos positivos en una enfermedad con prevalencia de 0,1% no significa necesariamente estar enfermo.

**F122 · Falacia del fiscal** *(Prosecutor's fallacy)*
Severidad: ◆◆◆
Forma: Confundir la probabilidad de la prueba dado que el sujeto es inocente con la inversa
Confundir P(evidencia | inocente) con P(inocente | evidencia).
Ejemplo: "La probabilidad de que un inocente deje esta huella es 1 en un millón. Luego, la probabilidad de que sea inocente es 1 en un millón." (ignora la prevalencia de sospechosos)

**F123 · Falacia del defensor** *(Defense attorney's fallacy)*
Severidad: ◆◆◆
Forma: Descartar una prueba porque individualmente tiene baja probabilidad de acierto
El error inverso: porque la evidencia aislada es improbable condicionalmente, el crimen no ocurrió o el acusado es inocente.
Ejemplo: 'Si la huella coincide con 1 de cada 100 personas, hay miles de sospechosos, luego la huella no sirve.' (ignora otros filtros)
**F124 · Falacia de la conjunción** *(Conjunction fallacy — Kahneman & Tversky, problema de Linda)*
Severidad: ◆◆◆
Forma: P(A ∧ B) se juzga más probable que P(A) debido a la coherencia del relato
Juzgar una conjunción de eventos como más probable que uno de sus componentes por sola coherencia narrativa.
Ejemplo: Juzgar que "Linda es cajera de banco y feminista" es más probable que "Linda es cajera de banco", cuando matemáticamente no puede serlo.

**F125 · Falacia de la disyunción**
Severidad: ◆◆
Forma: Subestimar la probabilidad de que ocurra al menos uno de varios fallos posibles
Subestimar la probabilidad de que al menos uno de varios eventos independientes ocurra.
Ejemplo: 'Es poco probable que falle el motor, o el ala, o el piloto, así que el avión es seguro.' (la probabilidad de fallo total es la suma de riesgos)
**F126 · Paradoja de Simpson como trampa argumental** *(Simpson's paradox misuse)*
Severidad: ◆◆◆
Forma: Usar datos agregados para ocultar que la tendencia es opuesta en cada subgrupo
Usar datos agregados para ocultar tendencias que invierten su dirección en subgrupos, o viceversa.
Ejemplo: "El hospital A tiene mayor tasa de mortalidad que el B, luego el A es peor." (sin controlar por gravedad de los casos admitidos)

**F127 · Falacia de la precisión** *(Precision fallacy)*
Severidad: ◆◆
Forma: Asociar la precisión de una cifra decimal con la veracidad de la afirmación
Usar cifras muy precisas para dar apariencia de rigor a una estimación fundamentalmente especulativa.
Ejemplo: "Esto aumentará el PIB un 3,742%."

**F128 · Regresión a la media ignorada** *(Regression to the mean)*
Severidad: ◆◆◆
Forma: Ignorar que tras un valor extremo, el siguiente tiende a estar más cerca de la media
Atribuir causalidad a fenómenos que son simplemente efecto estadístico de regresión hacia la media.
Ejemplo: "Le grité y mejoró su actuación" (ignorando que tras un rendimiento muy bajo, la recuperación es estadísticamente esperable sin intervención).

**F129 · Falacia lúdica** *(Ludic fallacy — Taleb)*
Severidad: ◆◆◆
Forma: Tratar situaciones de la vida real como si fueran juegos con reglas y probabilidades fijas
Aplicar modelos probabilísticos de sistemas cerrados y bien definidos (juegos de azar) a sistemas abiertos y complejos del mundo real donde las distribuciones son desconocidas.
Ejemplo: 'En el casino las reglas son claras, así que en la bolsa el riesgo debe ser igual de predecible.'
**F130 · Falacia narrativa** *(Narrative fallacy — Taleb)*
Severidad: ◆◆
Forma: Unir hechos dispersos en un relato coherente y asumir que esa coherencia prueba causalidad
Construir relatos causales coherentes sobre series de eventos que contienen alta aleatoriedad, sobreestimando la causalidad e infraestimando el azar.
Ejemplo: 'La empresa quebró porque el director cambió de coche, todo encaja en mi historia sobre su arrogancia.'
**F131 · Falacia del porcentaje sin base**
Severidad: ◆◆◆
Forma: Citar aumentos porcentuales sin mencionar la cantidad absoluta de partida
Usar porcentajes sin mencionar la base, o comparar porcentajes de bases distintas.
Ejemplo: "¡Aumentamos un 100% los casos!" (de 1 a 2 casos)

**F132 · Falacia del 95%** *(P-value fallacy)*
Severidad: ◆◆◆
Forma: Asumir que la significación estadística equivale a importancia real o verdad absoluta
Confundir significación estadística (p < 0,05) con relevancia práctica, tamaño del efecto o verdad sustantiva.
Ejemplo: 'Hay una correlación con p=0.04 entre comer pepinos y ser rubio, luego el pepino aclara el pelo.'
**F133 · HARKing** *(Hypothesizing After Results are Known)*
Severidad: ◆◆◆
Forma: Presentar una hipótesis creada tras ver los resultados como si hubiera sido la original
Presentar hipótesis formuladas después de ver los datos como si hubieran sido planteadas antes, falseando el proceso científico.
Ejemplo: 'Vi que las plantas crecieron más con música, así que dije que mi hipótesis era que la música ayudaba.'


---

### BLOQUE 14 · Falacias de inferencia inductiva general

**F134 · Enumeración perfecta falsa** *(False complete enumeration)*
Severidad: ◆◆◆
Forma: Ofrecer una lista de opciones como si fuera completa cuando faltan alternativas
Presentar una lista como exhaustiva cuando no lo es.
Ejemplo: 'Solo hay tres tipos de personas: las que mandan, las que obedecen y las que se rebelan.'
**F135 · Inducción por confirmación exclusiva**
Severidad: ◆◆◆
Forma: Diseñar el método de investigación para que solo encuentre pruebas a favor
Buscar solo casos que confirmen una hipótesis y no casos que pudieran falsarla (relacionado con el sesgo de confirmación, pero como error metodológico deliberado).
Ejemplo: 'Para mi informe, solo he entrevistado a los clientes que compraron el producto dos veces.'
**F136 · Falacia de la regla sin excepción**
Severidad: ◆◆
Forma: Aplicar una regla de forma absoluta sin considerar excepciones obvias
Extender una generalización empírica a casos límite donde claramente no aplica sin reconocer la excepción.
Ejemplo: 'Mentir es malo, así que no mientas al secuestrador sobre dónde están los rehenes.'


---

## PARTE VIII — FALACIAS EPISTÉMICAS

---

**F137 · Proyección mental** *(Mind projection fallacy — Jaynes)*
Severidad: ◆◆◆
Forma: Creer que las estructuras de nuestro razonamiento son leyes físicas del universo
Asumir que las propiedades del propio mapa cognitivo son propiedades del territorio real.
Ejemplo: "No puedo concebir un universo sin propósito, luego el universo tiene propósito."

**F138 · Ilusión de comprensión explicativa** *(Illusion of explanatory depth)*
Severidad: ◆◆
Forma: Confundir la familiaridad con un término con el conocimiento profundo del objeto
Creer que se comprende un mecanismo en detalle cuando en realidad solo se tiene una comprensión superficial, y argumentar desde esa falsa comprensión.
Ejemplo: 'Sé cómo funciona una cremallera.' (pero no puede explicar la mecánica de los dientes)
**F139 · Vocabulario técnico como evidencia** *(Technobabble fallacy)*
Severidad: ◆◆◆
Forma: Sustituir el razonamiento lógico por el uso intensivo de terminología técnica
Usar terminología especializada o jerga científica como sustituto de argumentación.
Ejemplo: "El efecto cuántico de tunelamiento explica la homeopatía."

**F140 · Conocimiento por descripción como conocimiento directo** *(Russell)*
Severidad: ◆◆
Forma: Equiparar la lectura de manuales o teoría con la adquisición de una destreza práctica
Tratar el conocimiento proposicional sobre algo (saber que) como equivalente al conocimiento experiencial o directo (saber cómo / conocer).
Ejemplo: 'He leído diez libros sobre aviones, ya sé pilotar.'
**F141 · Razonamiento motivado como falacia** *(Motivated reasoning)*
Severidad: ◆◆◆
Forma: Cambiar el nivel de exigencia de pruebas según si nos gusta o no la conclusión
Ajustar el umbral de evidencia necesario según el resultado que se quiere obtener: exigir mucha evidencia para lo que se quiere rechazar y poca para lo que se quiere aceptar.
Ejemplo: 'Acepto este rumor con una fuente, pero pido tres estudios para creer lo contrario.'
**F142 · Doble rasero epistémico** *(Double standard)*
Severidad: ◆◆◆
Forma: Usar criterios de evaluación diferentes para mi bando y para el bando contrario
Aplicar criterios de evidencia distintos a casos análogos según conveniencia ideológica o personal.
Ejemplo: 'Mi religión es fe; la tuya es superstición que requiere pruebas.'
**F143 · Falso balance** *(Both-sidesism / False equivalence)*
Severidad: ◆◆◆
Forma: Presentar dos opiniones como equivalentes cuando una carece de respaldo
Presentar dos posiciones como igualmente válidas o igualmente respaldadas por evidencia cuando no lo son.
Ejemplo: Dar el mismo espacio mediático al consenso científico climático y a posiciones negacionistas marginales.

**F144 · Deepity** *(Dennett)*
Severidad: ◆◆
Forma: Afirmación que suena profunda pero es un juego de palabras vacío o trivial
Afirmaciones que parecen profundas pero son triviales en sentido literal y falsas o vacías en sentido profundo.
Ejemplo: "El amor es solo una palabra" (trivialmente verdadero en sentido literal; si se entiende como algo más profundo, es falso o carece de contenido).

**F145 · Profundidad ilusoria de la paradoja** *(Paradox profundity fallacy)*
Severidad: ◆
Forma: Presentar una contradicción terminológica como una revelación mística
Presentar contradicciones aparentes como insights profundos en lugar de resolverlas o identificarlas como genuinas paradojas.
Ejemplo: 'Para ganar, primero hay que perder.'


---

## PARTE IX — FALACIAS DE RAZONAMIENTO MORAL Y ÉTICO

---

**F146 · Falacia naturalista** *(Naturalistic fallacy — G.E. Moore)*
Severidad: ◆◆◆
Forma: Lo que es natural es por definición moralmente correcto o deseable
Inferir lo que "debe ser" a partir de lo que "es": confundir hecho con valor, ser con deber ser.
Guillotina de Hume: la brecha lógica entre enunciados descriptivos y normativos.
Ejemplo: "Los animales más fuertes dominan a los débiles en la naturaleza. Luego, el más fuerte debe dominar."

**F147 · Falacia moralista** *(Moralistic fallacy)*
Severidad: ◆◆◆
Forma: Porque algo debería ser así moralmente, se concluye que es así en la realidad
Lo inverso: inferir cómo son las cosas empíricamente a partir de cómo deberían ser moralmente.
Ejemplo: "Sería injusto que existieran diferencias innatas de capacidad, luego no existen."

**F148 · Falacia del consenso moral**
Severidad: ◆◆
Forma: Lo que la mayoría considera bueno se toma como medida de la moralidad
Lo que la mayoría considera correcto es correcto. Distinta del ad populum porque opera específicamente en el dominio normativo.
Ejemplo: 'Si la mayoría aprueba la tortura, entonces es moralmente aceptable.'
**F149 · Relativismo moral como conclusión empírica**
Severidad: ◆◆◆
Forma: La observación de distintas costumbres se usa para negar la posibilidad de ética objetiva
"Culturas distintas tienen distintos valores morales" (hecho). ∴ "No hay valores morales objetivos" (conclusión normativa). Salto injustificado del hecho al valor (o a la negación del valor).
Ejemplo: 'Cada cultura tiene sus valores, por tanto no hay verdades morales.'
**F150 · Omission bias como falacia ética**
Severidad: ◆◆
Forma: Juzgar un daño activo como peor que uno pasivo con el mismo resultado
Juzgar una acción dañina como moralmente peor que una omisión igualmente dañina por el solo hecho de ser activa.
Relevante en bioética, derecho y política pública.
Ejemplo: 'Es mejor no vacunar y que el niño enferme, que vacunarlo y que tenga un efecto secundario.'
**F151 · Doble efecto mal aplicado** *(Doctrine of double effect misuse)*
Severidad: ◆◆
Forma: Justificar un daño como 'daño colateral' cuando es en realidad el medio para el fin
El principio del doble efecto usado para justificar consecuencias negativas que en realidad son el fin buscado, no el efecto secundario no deseado.
Ejemplo: 'Bombardeamos civiles para que el gobierno se rinda; sus muertes son un efecto secundario.'
**F152 · Falacia de la persona completa** *(Whole-person fallacy)*
Severidad: ◆◆
Forma: Evaluar la calidad moral de toda una vida por un único acto aislado
Evaluar el comportamiento moral de una persona en función de una sola acción, positiva o negativa, ignorando el patrón completo.
Ejemplo: 'Mintió una vez, por tanto es un mentiroso en todo lo que hace.'
**F153 · Falacia de la legalidad moral**
Severidad: ◆◆◆
Forma: Confundir el cumplimiento de la ley positiva con la corrección ética
Lo legal es moralmente aceptable; lo ilegal es moralmente reprobable. Confunde norma jurídica con norma ética.
Ejemplo: 'Era legal tener esclavos, por tanto era moralmente correcto en ese tiempo.'
**F154 · Universalización kantiana incorrecta**
Severidad: ◆◆
Forma: Extender el principio de universalización a situaciones donde el contexto es clave
Usar la universalizabilidad como único criterio moral o aplicarla mecánicamente a casos donde Kant mismo requeriría matización.
Ejemplo: 'Si todos robáramos pan, no habría panaderías; por tanto, el que muere de hambre no debe robar pan.'
**F155 · Falacia del hecho bruto como justificación**
Severidad: ◆◆◆
Forma: La existencia histórica o actual de un hecho se usa como prueba de que es correcto
Presentar la mera existencia de algo como justificación de su continuación o de su corrección.
Ejemplo: "La pobreza siempre ha existido, luego es inevitable y no hay que combatirla."

---

## PARTE X — FALACIAS DIALÉCTICAS Y PRAGMÁTICAS
*(Walton, van Eemeren & Grootendorst — pragma-dialéctica)*

---

**F156 · Desplazamiento de la carga de prueba** *(Burden of proof reversal)*
Severidad: ◆◆◆
Forma: El proponente de una idea inusual exige que los demás demuestren que es falsa
Quien afirma debe probar. Exigir al otro que pruebe la negación es invertir ilegítimamente la carga.
Ejemplo: 'Hay un dragón en mi garaje; demuéstrame tú que no existe.'
**F157 · Argumento desde las consecuencias** *(Argumentum ad consequentiam)*
Severidad: ◆◆◆
Forma: Negar la verdad de una proposición porque sus consecuencias serían desagradables
Argumentar que algo es verdad (o falso) porque sus consecuencias serían deseables (o indeseables).
Ejemplo: "El libre albedrío debe existir porque si no, nadie sería responsable de nada."

**F158 · Argumento desde las consecuencias prácticas** *(Pragmatic fallacy)*
Severidad: ◆◆
Forma: Aceptar una idea como verdadera solo porque creer en ella es provechoso
Aceptar una proposición porque creer en ella tiene buenos efectos prácticos, independientemente de su verdad.
Ejemplo: 'Creer en el destino me hace feliz, por tanto el destino es real.'
**F159 · Mover los postes** *(Moving the goalposts)*
Severidad: ◆◆◆
Forma: Cambiar las condiciones de victoria de un debate una vez que han sido cumplidas
Cambiar los criterios de éxito o las condiciones de aceptación de un argumento una vez que han sido satisfechos.
Ejemplo: 'Has probado que flota, pero ¿has probado que resiste el fuego?'
**F160 · Aplazamiento infinito** *(Infinite regress defense)*
Severidad: ◆◆
Forma: Exigir una cadena infinita de justificaciones para evitar aceptar un punto de partida
Responder a cada cuestionamiento con otro cuestionamiento, sin ofrecer nunca una base positiva.
Ejemplo: '¿Quién creó al creador del creador?'
**F161 · Silencio como consentimiento** *(Silence implies consent)*
Severidad: ◆◆
Forma: Interpretar la falta de oposición inmediata como un acuerdo total
Interpretar la falta de respuesta o refutación como aceptación tácita.
Ejemplo: 'No dijiste que no a mi propuesta, así que asumo que estás de acuerdo.'
**F162 · Distracción dialéctica**
Severidad: ◆◆◆
Forma: Descalificar todo un argumento basándose en la refutación de un detalle mínimo
Responder a la totalidad del argumento atacando un detalle periférico y tratar esa respuesta parcial como si fuera una refutación completa.
Ejemplo: 'Has errado en una cifra del informe, por tanto todo el informe es basura.'
**F163 · Galileo gambit** *(Falacia de la persecución como validación)*
Severidad: ◆◆◆
Forma: Creer que la crítica o el rechazo social son pruebas de que se posee la verdad
"Dijeron que Galileo estaba loco, y tenía razón. A mí también me critican, luego también tengo razón."
Confunde la persecución con la validez epistémica.
Ejemplo: 'Me llaman loco como a Galileo, por tanto tengo razón.'
**F164 · Pregunta retórica como argumento**
Severidad: ◆◆
Forma: Formular una pregunta cuya respuesta se da por sentada como argumento
Usar una pregunta retórica como si constituyera evidencia o argumento sustantivo.
Ejemplo: "¿Acaso no es obvio que el capitalismo ha fracasado?" (la obviedad no está justificada)

**F165 · Compromiso explotado** *(Commitment exploitation — Walton)*
Severidad: ◆◆
Forma: Utilizar declaraciones antiguas del interlocutor para forzarle a una conclusión actual
Usar los compromisos previos del interlocutor para forzarle a posiciones que no aceptaría en contexto fresco, ignorando la evolución legítima del pensamiento.
Ejemplo: 'Dijiste que te gustaba el riesgo, así que no te quejes por perder tu dinero.'
**F166 · Uso indebido del contexto de diálogo** *(Context shift fallacy — Walton)*
Severidad: ◆◆
Forma: Aplicar tácticas de un contexto de diálogo a otro donde son inapropiadas
Adoptar tácticas propias de un tipo de diálogo (negociación, litigio) en otro donde no son pertinentes (investigación, deliberación filosófica).
Ejemplo: 'En una negociación se regatea, así que pactemos el valor de la gravedad.'
**F167 · Posición de autoridad en el diálogo**
Severidad: ◆◆◆
Forma: Rechazar un argumento solo por el cargo o posición de quien lo emite
En un diálogo deliberativo, rechazar la contribución del interlocutor por su estatus social o jerárquico, no por su contenido.
Ejemplo: 'Soy tu superior, por tanto mi lógica es mejor.'
**F168 · Dilución del argumento** *(Argument dilution)*
Severidad: ◆◆
Forma: Combinar una tesis fuerte con varias débiles para ocultar la debilidad del conjunto
Mezclar la conclusión bien fundamentada con otras conclusiones débiles o no probadas, de modo que el conjunto parece más débil o más fuerte de lo que es.
Ejemplo: 'Esta ley es buena por economía, justicia y porque me gusta el logo.'


---

## PARTE XI — FALACIAS DE DEFINICIÓN Y TAXONOMÍA

---

**F169 · Definición por ejemplo** *(Definition by example — ostensive fallacy)*
Severidad: ◆
Forma: Definir un concepto general citando solamente algunos ejemplos particulares
Tratar ejemplos como si fueran definiciones completas y extraer consecuencias generales de características accidentales del ejemplo.
Ejemplo: 'La libertad es poder votar y elegir marca de champú.'
**F170 · Definición negativa exclusiva**
Severidad: ◆
Forma: Intentar definir algo diciendo solo lo que no es, sin aportar rasgos positivos
Definir algo únicamente por lo que no es, sin especificar lo que sí es, y luego argumentar desde esa definición como si fuera positiva.
Ejemplo: 'La felicidad es no tener deudas y no tener hambre.'
**F171 · Cambio de significado durante el argumento** *(Shifting meaning)*
Severidad: ◆◆◆
Forma: Usar una palabra con un sentido en la premisa y con otro diferente en la conclusión
Usar un término con un significado al inicio del argumento y cambiarlo sutilmente a lo largo de él, variante de la equivocación.
Ejemplo: 'La fe es sustancia. Tengo fe en el bus. Luego el bus es una sustancia.'
**F172 · Argumento de la etiqueta** *(Label argument)*
Severidad: ◆◆
Forma: Creer que haberle puesto nombre a un problema es haberlo explicado
Creer que nombrar algo equivale a explicarlo o a justificar una conclusión sobre ello.
Ejemplo: "¿Por qué duerme el opio? Por su virtud dormitiva." (Molière; la etiqueta no añade contenido)

---

## PARTE XII — FALACIAS ESPECÍFICAS DE DOMINIO

---

### BLOQUE 15 · Científicas

**F173 · Falacia de la excepción confirmatoria** *(Ad hoc exception)*
Severidad: ◆◆◆
Forma: Salvar una regla de un contraejemplo creando una excepción sin base lógica
Salvar una teoría del contraejemplo añadiendo excepciones ad hoc sin base independiente.
Ejemplo: 'Todos deben pagar, menos los de mi calle por su buen carácter.'
**F174 · Cherry-picking de estudios** *(Publication bias exploitation)*
Severidad: ◆◆◆
Forma: Mencionar solo el estudio que conviene de entre toda la literatura existente
Selección sesgada de literatura científica: citar solo los estudios favorables, ignorar los desfavorables o los que no se publicaron.
Ejemplo: 'Un estudio de 1970 dice que el tabaco es bueno, ignoremos el resto.'
**F175 · Confusión modelo/realidad** *(Map-territory confusion en ciencia)*
Severidad: ◆◆◆
Forma: Confundir las predicciones de una simulación con los hechos del mundo real
Tratar el modelo matemático o computacional como si fuera la realidad que describe, sin considerar sus supuestos y limitaciones.
Ejemplo: 'El modelo dice que no habrá pobreza, así que el problema está resuelto.'
**F176 · Extrapolación más allá del dominio** *(Overgeneralization of findings)*
Severidad: ◆◆◆
Forma: Aplicar los hallazgos de un campo a otro muy distinto sin validación
Extender los resultados de un estudio más allá de la población, condiciones o fenómenos para los que fue diseñado.
Ejemplo: Generalizar hallazgos en estudiantes universitarios occidentales a toda la humanidad (WEIRD bias).

**F177 · Falacia de la analogía de laboratorio** *(Laboratory-to-world fallacy)*
Severidad: ◆◆
Forma: Creer que lo que sucede en el laboratorio ocurrirá igual en la complejidad del mundo
Asumir que los resultados obtenidos en condiciones controladas de laboratorio se trasladan sin modificación a condiciones del mundo real.
Ejemplo: 'Mata bacterias en el vidrio, así que beberlo curará tu infección.'
**F178 · Cuestionamiento del consenso sin alternativa** *(Contrarianism fallacy)*
Severidad: ◆◆
Forma: Criticar el consenso sin proponer una teoría que explique mejor los datos
Rechazar el consenso científico sin ofrecer una alternativa mejor fundamentada, tratando el cuestionamiento mismo como argumento suficiente.
Ejemplo: 'No creo en la evolución por sus fallos, pero no tengo otra explicación.'


---

### BLOQUE 16 · Económicas

**F179 · Falacia de la composición económica** *(Paradox of thrift como falacia)*
Severidad: ◆◆◆
Forma: Asumir que lo que beneficia a un individuo beneficia a toda la economía
Lo que es racional para un individuo (ahorrar) no lo es necesariamente para el conjunto cuando todos lo hacen simultáneamente.
Ejemplo: 'Si yo ahorro gano; si todos ahorramos a la vez, el consumo cae y la economía se hunde.'
**F180 · Falacia del crecimiento infinito**
Severidad: ◆◆
Forma: Suponer que una tendencia de crecimiento puede mantenerse sin límites físicos
Asumir que el crecimiento económico puede ser indefinido en un sistema físicamente finito.
Ejemplo: 'Podemos crecer un 3% anual para siempre en este planeta.'
**F181 · Apelación a las consecuencias económicas como verdad**
Severidad: ◆◆
Forma: Concluir que una política es verdadera o justa solo porque genera beneficios
Un argumento o política es verdadero/correcto porque su aplicación sería rentable.
Ejemplo: 'Es rentable engañar sobre las baterías, por tanto es la política correcta.'
**F182 · Costo hundido** *(Sunk cost fallacy)*
Severidad: ◆◆◆
Forma: Continuar una acción fallida solo por los recursos ya perdidos en ella
Continuar con una acción solo porque ya se han invertido recursos en ella, ignorando la racionalidad prospectiva.
Ejemplo: "Ya he pagado el 70% del máster; tengo que terminarlo aunque no me aporte nada."

**F183 · Falacia del costo de oportunidad ignorado**
Severidad: ◆◆
Forma: Juzgar una opción sin valorar lo que se pierde al no elegir la otra
Evaluar una decisión sin considerar lo que se renuncia al tomarla.
Ejemplo: "Este proyecto no cuesta nada; usamos recursos propios." (ignorando que esos recursos podrían usarse de otra manera)

**F184 · Falacia de la distribución como suma cero** *(Zero-sum fallacy)*
Severidad: ◆◆◆
Forma: Creer que para que alguien gane, otro debe perder necesariamente
Asumir que lo que gana una parte necesariamente lo pierde otra, en contextos donde el valor puede crearse o destruirse.
Ejemplo: 'Si ellos se enriquecen, nosotros nos empobrecemos.'


---

### BLOQUE 17 · Jurídico-políticas

**F185 · Ad hominem de la fuente legal**
Severidad: ◆◆
Forma: Rechazar una norma solo por el grupo político que la ha redactado
Descalificar una ley o norma por su origen político en lugar de por su contenido.
Ejemplo: 'Esa ley es mala porque la hizo la oposición.'
**F186 · Pendiente resbaladiza jurídica**
Severidad: ◆◆
Forma: Argumentar que una pequeña concesión legal destruirá todo el orden jurídico
"Si permitimos X legalmente, el sistema no podrá contener Y."
Ejemplo: 'Si perdonamos esta multa, mañana nadie respetará ninguna ley.'
**F187 · Apelación al precedente irrelevante**
Severidad: ◆◆◆
Forma: Usar un caso del pasado como guía para uno actual que es esencialmente distinto
Aplicar precedente jurídico a un caso sin verificar que las propiedades jurídicamente relevantes sean análogas.
Ejemplo: 'Se construyó en la costa en 1950, así que yo puedo construir hoy en la reserva.'
**F188 · Falacia de la norma como descripción**
Severidad: ◆◆◆
Forma: Tomar un enunciado del deber ser como si fuera una descripción de la realidad
Confundir enunciados normativos ("debes pagar impuestos") con enunciados descriptivos ("la gente paga impuestos").
Ejemplo: 'La ley dice que somos iguales, por tanto no hay racismo.'
**F189 · Falacia del mandato popular** *(Mandate fallacy)*
Severidad: ◆◆
Forma: Creer que ganar un cargo da permiso para imponer cada detalle de un programa
Interpretar una victoria electoral estrecha como respaldo amplio a todas las políticas del vencedor.
Ejemplo: 'He ganado, así que el pueblo quiere exactamente todo lo que propuse.'
**F190 · Apelación al estado de naturaleza** *(State of nature fallacy)*
Severidad: ◆◆
Forma: Justificar leyes actuales basándose en una visión idealizada del hombre primitivo
Usar una descripción del estado de naturaleza (a menudo ficticia o idealizada) para justificar arreglos sociales actuales.
Ejemplo: 'En la naturaleza no hay propiedad, así que debemos abolirla hoy.'


---

### BLOQUE 18 · Psicológicas y cognitivas sistematizadas

**F191 · Sesgo de anclaje argumentativo** *(Anchoring bias as fallacy)*
Severidad: ◆◆
Forma: Quedarse atrapado en la primera cifra mencionada al evaluar una oferta
Tomar el primer dato mencionado como referencia no cuestionada y construir el argumento sobre él.
Ejemplo: 'Costaba 1000, por 800 es un chollo, aunque valga 100.'
**F192 · Efecto de encuadre** *(Framing effect as fallacy)*
Severidad: ◆◆◆
Forma: Presentar la misma información de forma positiva o negativa para influir
La misma información presentada con encuadre diferente conduce a evaluaciones contradictorias; usar el encuadre deliberadamente para manipular la conclusión.
Ejemplo: '90% libre de grasa' vs '10% de grasa'.
**F193 · Efecto halo argumentativo** *(Halo effect)*
Severidad: ◆◆
Forma: Valorar el argumento de alguien basándose en su carisma o atractivo físico
Asumir que porque alguien es bueno en un ámbito, sus juicios en otro ámbito también son fiables.
Ejemplo: 'Es un actor guapo, su opinión sobre geopolítica debe ser acertada.'
**F194 · Sesgo de status quo como argumento** *(Status quo bias)*
Severidad: ◆◆
Forma: Defender la situación actual solo por miedo al cambio o inercia
Preferir el estado actual de las cosas como argumento en sí mismo, sin justificación adicional de su superioridad.
Ejemplo: 'Siempre usamos papel, no hay razón para cambiar a digital.'
**F195 · Falacia del punto de referencia** *(Reference point fallacy)*
Severidad: ◆◆
Forma: Ignorar la escala total y centrarse solo en la variación relativa
Evaluar una situación exclusivamente desde el punto de referencia actual, ignorando la escala absoluta.
Ejemplo: "Solo perdimos el 10% de los empleos" (puede ser catastrófico en términos absolutos)

---

## PARTE XIII — FALACIAS DE LA ERA DIGITAL Y MEDIÁTICA
*(Categoría emergente; epistemología de las redes)*

---

**F196 · Screenshot como prueba** *(Screenshot fallacy)*
Severidad: ◆◆◆
Forma: Aceptar una captura de pantalla como verdad absoluta sin verificarla
Tratar una imagen descontextualizada como evidencia sin verificar autenticidad, contexto ni fuente.
Ejemplo: 'Tengo el pantallazo del tweet, así que el ministro dijo eso.'
**F197 · Trending como verdad** *(Virality fallacy)*
Severidad: ◆◆◆
Forma: Asociar el número de visualizaciones con la veracidad del contenido
Lo que es viral o tendencia refleja la realidad o tiene mayor probabilidad de ser verdad.
Ejemplo: 'Tiene millones de visitas, algo de cierto habrá en el video.'
**F198 · Cámara de eco como representatividad** *(Echo chamber fallacy)*
Severidad: ◆◆◆
Forma: Creer que lo que dice mi grupo de redes sociales es lo que piensa todo el mundo
Lo que se ve repetidamente en el propio entorno informativo refleja la distribución real de opiniones en la sociedad.
Ejemplo: 'En mi Facebook todos odian la ley, así que será un fracaso.'
**F199 · Cherry-picking algorítmico** *(Algorithmic selection fallacy)*
Severidad: ◆◆◆
Forma: Confundir la selección personalizada de un algoritmo con la realidad general
Confundir el perfil de resultados que un algoritmo selecciona (personalizado) con una muestra representativa del universo informativo.
Ejemplo: 'Solo veo noticias de robos, el mundo es un caos total.'
**F200 · Like como endorsement epistémico**
Severidad: ◆◆
Forma: Creer que una idea es válida porque tiene muchos 'me gusta'
Tratar el apoyo cuantificado (likes, shares, retweets) como validación del contenido o evidencia de su veracidad.
Ejemplo: 'Esa dieta tiene 50.000 likes, debe ser muy sana.'
**F201 · Fuente verificada como fuente veraz**
Severidad: ◆◆◆
Forma: Confundir la identidad verificada de una cuenta con la verdad de lo que publica
Confundir la verificación de identidad (checkmark o credenciales) con la fiabilidad o veracidad del contenido publicado.
Ejemplo: 'La cuenta tiene el check azul, así que la noticia es real.'
**F202 · Palimpsesto digital** *(Digital palimpsest fallacy)*
Severidad: ◆◆
Forma: Usar textos antiguos y superados para atacar la posición presente de alguien
Citar capturas de texto editado o corregido como si representaran la versión actual o definitiva de una posición.
Ejemplo: 'Esto escribiste hace 20 años, eso es lo que piensas hoy.'
**F203 · Deepfake como argumento** *(Synthetic media fallacy)*
Severidad: ◆◆◆
Forma: Tomar un video generado por IA como una prueba irrefutable de un suceso
Usar contenido sintético (audio, video, imagen generada artificialmente) como evidencia de eventos reales.
Ejemplo: 'He visto el video del soborno, es la prueba.' (siendo un deepfake)
**F204 · Autoridad de la plataforma**
Severidad: ◆◆
Forma: Creer que si algo fuera falso, la red social ya lo habría borrado
Tratar el hecho de que un contenido no haya sido eliminado o etiquetado por la plataforma como validación de su veracidad.
Ejemplo: 'Si fuera mentira, la plataforma ya lo habría quitado.'


---

## PARTE XIV — FALACIAS DE SEGUNDO ORDEN Y META-FALACIAS

---

**F205 · Falacia del detector de falacias** *(Ad logicam / Fallacy fallacy)*
Severidad: ◆◆◆
Forma: Concluir que una idea es falsa solo porque se ha defendido con una falacia
Concluir que una tesis es falsa porque el argumento que la defiende contiene una falacia. La verdad de la conclusión es independiente de la validez del argumento que la sostiene.
Ejemplo: "Tu argumento para demostrar que 2+2=4 es un non sequitur. Luego, 2+2≠4."

**F206 · Etiqueta de falacia sin demostración** *(Name-calling fallacy)*
Severidad: ◆◆◆
Forma: Ponerle el nombre de una falacia a un argumento sin explicar por qué lo es
Nombrar una falacia sin demostrar que el argumento en cuestión la comete efectivamente.
Ejemplo: 'Eso es un hombre de paja.' (sin demostrar la deformación)
**F207 · Falacia de la falacia de la falacia** *(Meta-meta fallacy)*
Severidad: ◆◆
Forma: Usar la acusación de 'falacia del detector' para proteger una falacia propia
Invocar la "falacia del detector de falacias" para desestimar señalamientos legítimos de falacias reales.
Ejemplo: 'Me acusas de ad hominem, eso es la falacia del detector, así que mi insulto vale.'
**F208 · Formalismo estricto** *(Straw-formalist fallacy)*
Severidad: ◆◆
Forma: Rechazar cualquier razonamiento que no esté en formato lógico perfecto
Exigir formalización lógica completa de argumentos cotidianos y rechazarlos si no la tienen, ignorando que el lenguaje natural admite argumentación válidamente informal.
Ejemplo: 'No es un silogismo perfecto, así que no es válido.'
**F209 · Solución perfecta** *(Nirvana fallacy / Perfect solution fallacy)*
Severidad: ◆◆◆
Forma: Descartar una solución útil porque no alcanza la perfección ideal
Rechazar una solución porque no es perfecta, comparándola con un ideal imposible en lugar de con las alternativas disponibles.
Ejemplo: "Este tratado de paz no resuelve todos los conflictos, luego no sirve de nada."

**F210 · Dilación de Galileo** — véase F163
Forma: Demandar pruebas adicionales de forma infinita para no aceptar una verdad incómoda
Ejemplo: 'Esperemos 50 años más de estudios sobre el tabaco para estar seguros.'
**F211 · Falacia de la dilución argumental** *(Argument dilution — Walton)*
Severidad: ◆◆
Forma: Defender una versión suave de una tesis y luego actuar como si se hubiera probado la versión extrema
Tomar la versión más débil posible de la propia tesis para hacerla irrefutable, luego reclamar implícitamente que se ha defendido la versión fuerte. Variante formal del motte and bailey.
Ejemplo: 'Hay dudas sobre el informe, luego el informe es una basura total.'


---

## PARTE XV — FALACIAS VISUALES Y NO VERBALES
*(Falacias en argumentación que no es exclusivamente verbal)*

---

**F212 · Escala manipulada en gráfica** *(Truncated graph fallacy)*
Severidad: ◆◆◆
Forma: Manipular los ejes de un gráfico para que cambios mínimos parezcan enormes
Usar ejes que no empiezan en cero o tienen escalas no lineales para exagerar o minimizar visualmente diferencias reales.
Ejemplo: 'El eje empieza en 9.8%, así que una bajada de 0.1% parece del 50%.'
**F213 · Correlación visual espuria** *(Visual correlation fallacy)*
Severidad: ◆◆◆
Forma: Presentar dos gráficas similares para sugerir que una causa la otra
Presentar dos gráficas con tendencias similares como si demostraran causalidad entre las variables.
Ejemplo: 'Sube el consumo de helado y suben los ataques de tiburón, luego el helado atrae tiburones.'
**F214 · Selección de imagen sesgada** *(Framing by image selection)*
Severidad: ◆◆◆
Forma: Elegir una foto de un caso raro para representar a todo un colectivo
Elegir imágenes que representan un caso excepcional como si fuera el caso típico (o viceversa) para apoyar una conclusión.
Ejemplo: 'Poner la foto de un niño sonriendo para decir que no hay guerra.'
**F215 · Falacia del porcentaje visual** *(Proportional representation fallacy)*
Severidad: ◆◆
Forma: Dibujar proporciones en un gráfico que no coinciden con los datos numéricos
Usar representaciones gráficas (tartas, barras) donde las proporciones visuales no corresponden a las proporciones numéricas reales.
Ejemplo: 'Un sector del 20% que ocupa la mitad del gráfico circular.'
**F216 · Apelación visual a la autoridad** *(Visual appeal to authority)*
Severidad: ◆◆
Forma: Usar la vestimenta o el entorno para dar autoridad a un mensaje sin pruebas
Usar imágenes de personas con bata blanca, laboratorios, o símbolos de autoridad para reforzar afirmaciones sin evidencia adicional.
Ejemplo: 'Sale con bata blanca, así que su consejo de belleza es médico.'


---

## PARTE XVI — FALACIAS EN ARGUMENTACIÓN POR AUTORIDAD
*(Walton distingue al menos 6 esquemas; el inventario estándar los colapsa en uno)*

---

**F217 · Apelación a la autoridad fuera de dominio** — F051 (ya incluida)
Forma: Citar a un experto en un tema para validar su opinión en otro tema diferente
Ejemplo: 'Este Nobel de física dice que las vitaminas curan el cáncer.'
**F218 · Apelación a la autoridad sin credenciales verificables**
Severidad: ◆◆◆
Forma: Aceptar una afirmación médica o científica de alguien sin ninguna formación
La persona citada no tiene formación demostrable en el área relevante.
Ejemplo: 'Un anónimo dice que el limón cura todo, debe ser verdad.'
**F219 · Apelación a la autoridad en desacuerdo con su campo** *(Lone dissenter fallacy)*
Severidad: ◆◆◆
Forma: Citar a un único experto que disiente para ignorar el consenso de miles de especialistas
Citar al único experto discrepante como si su opinión equivaliera al consenso del campo.
Ejemplo: Citar a un climatólogo que niega el cambio climático como contrapeso al 97% del consenso.

**F220 · Apelación a la autoridad desactualizada**
Severidad: ◆◆
Forma: Usar citas de autoridades de hace siglos para contradecir la ciencia actual
Citar a una autoridad cuya posición data de cuando el campo no tenía la evidencia actual.
Ejemplo: 'Aristóteles decía que los pesados caen antes, así que es así.'
**F221 · Apelación a la autoridad interesada** *(Paid expert / Conflicted authority)*
Severidad: ◆◆◆
Forma: No mencionar que el experto que habla recibe dinero de la empresa interesada
Citar a un experto que tiene conflictos de interés directos sin declarar dichos conflictos.
Ejemplo: 'La petrolera dice que sus vertidos no dañan, su estudio lo confirma.'
**F222 · Autoridad por prominencia mediática** *(Media prominence fallacy)*
Severidad: ◆◆
Forma: Confundir la fama televisiva con el conocimiento experto en una materia
Tratar la visibilidad mediática de una persona como equivalente a su autoridad epistémica en un tema.
Ejemplo: 'Ese tertuliano sale mucho, debe saber mucho de virus.'


---

## PARTE XVII — FALACIAS DE ABDUCCIÓN AVANZADA

---

**F223 · Conspiranoia abductiva** *(Conspiratorial abduction)*
Severidad: ◆◆◆
Forma: Elegir una explicación llena de conspiraciones complejas frente a una sencilla y probada
Construir una hipótesis conspirativa como "inferencia a la mejor explicación" cuando la hipótesis requiere más supuestos ad hoc que la explicación convencional, violando la parsimonia.
Ejemplo: 'Es más fácil creer que todos mienten a que la Tierra es redonda.'
**F224 · Patrón en el ruido** *(Apophenia as fallacy)*
Severidad: ◆◆◆
Forma: Atribuir un propósito inteligente a una formación azarosa de la naturaleza
Percibir conexiones significativas entre eventos no relacionados y usarlas como evidencia abductiva.
Ejemplo: 'Esa nube tiene forma de cara, es una señal del destino.'
**F225 · Evidencia anecdótica como inferencia a la mejor explicación**
Severidad: ◆◆◆
Forma: Usar un suceso fortuito que le pasó a un conocido como prueba de una ley universal
Usar un caso único como base de una abducción que requeriría evidencia sistemática.
Ejemplo: 'A mi primo le funcionó el amuleto, la magia existe.'


---

## PARTE XVIII — FALACIAS RETÓRICAS CLÁSICAS ADICIONALES
*(Quintiliano, Cicerón, Rhetorica ad Herennium)*

---

**F226 · Congeries** *(Acumulación de argumentos débiles)*
Severidad: ◆◆
Forma: Presentar una montaña de argumentos flojos pensando que la cantidad hace la calidad
Acumular gran cantidad de argumentos débiles o irrelevantes con la esperanza de que el volumen supla la solidez.
Ejemplo: 'Tengo 50 indicios vagos, así que debe ser culpable.'
**F227 · Exordio tendencioso** *(Captatio benevolentiae excesiva)*
Severidad: ◆
Forma: Empezar un discurso insultando o alabando para nublar el juicio del oyente
Usar el exordio o introducción para predisponer al auditorio de forma que filtre toda la argumentación posterior.
Ejemplo: 'Antes de oír las mentiras de mi rival, veamos los hechos...'
**F228 · Apelación al carácter propio** *(Ethos fallacy)*
Severidad: ◆◆
Forma: Pedir que se acepte un argumento solo por la buena fama personal del que lo dice
Usar la propia reputación, honestidad o trayectoria como sustituto de la evidencia o el argumento.
Ejemplo: "Yo nunca he mentido; luego puedes creer lo que digo ahora sin cuestionarlo."

**F229 · Quaestio** *(Falacia de la pregunta implantada)*
Severidad: ◆◆◆
Forma: Colar una afirmación no probada dentro de una pregunta o frase para que se acepte sin pensar
Introducir subrepticiamente una pregunta o suposición en el discurso de manera que el auditorio la asuma sin haberla evaluado.
Ejemplo: 'Como todos sabemos que el Estado es ineficiente, analicemos el plan...'


---

## APÉNDICE I — TABLA ÍNDICE COMPLETA

| Código | Nombre | Sección | Severidad |
|--------|--------|---------|-----------|
| F001 | Afirmación del consecuente | Formal | ◆◆◆ |
| F002 | Negación del antecedente | Formal | ◆◆◆ |
| F003 | Cuatro términos | Formal | ◆◆◆ |
| F004 | Indistribución del término medio | Formal | ◆◆◆ |
| F005 | Ilícito mayor | Formal | ◆◆◆ |
| F006 | Ilícito menor | Formal | ◆◆◆ |
| F007 | Dos premisas negativas | Formal | ◆◆◆ |
| F008 | Dos premisas particulares | Formal | ◆◆◆ |
| F009 | Conclusión más fuerte que premisas | Formal | ◆◆◆ |
| F010 | Silogismo disyuntivo incompleto | Formal | ◆◆ |
| F011 | Generalización existencial incorrecta | Cuantificadores | ◆◆◆ |
| F012 | Error de cuantificador intercambiado | Cuantificadores | ◆◆◆ |
| F013 | Generalización de casos vacíos | Cuantificadores | ◆◆ |
| F014 | Error de modalidad básico | Modal | ◆◆◆ |
| F015 | Necesitación errónea | Modal | ◆◆◆ |
| F016 | Confusión de dicto / de re | Modal | ◆◆◆ |
| F017 | Posibilidad como probabilidad | Modal | ◆◆ |
| F018 | Determinismo retrospectivo | Modal | ◆◆ |
| F019 | Hombre enmascarado | Modal | ◆◆◆ |
| F020 | Non sequitur formal | Formal | ◆◆◆ |
| F021 | Afirmación de disyunto inclusivo | Formal | ◆◆ |
| F022 | Transitividad incorrecta | Formal | ◆◆◆ |
| F023 | Equivocación | Ambigüedad | ◆◆◆ |
| F024 | Anfibología | Ambigüedad | ◆◆ |
| F025 | Acento | Ambigüedad | ◆◆ |
| F026 | Composición lógica | Ambigüedad | ◆◆◆ |
| F027 | División lógica | Ambigüedad | ◆◆◆ |
| F028 | Composición verbal | Ambigüedad | ◆◆ |
| F029 | División verbal | Ambigüedad | ◆◆ |
| F030 | Figura de dicción | Ambigüedad | ◆ |
| F031 | Reificación | Ambigüedad | ◆◆ |
| F032 | Equivocación proceso/producto | Ambigüedad | ◆◆ |
| F033 | Confusión uso/mención | Ambigüedad | ◆◆◆ |
| F034 | Falacia etimológica | Ambigüedad | ◆◆ |
| F035 | Definición persuasiva | Definición | ◆◆ |
| F036 | Definición circular | Definición | ◆◆◆ |
| F037 | Definición demasiado amplia | Definición | ◆◆ |
| F038 | Definición demasiado estrecha | Definición | ◆◆ |
| F039 | Lenguaje cargado emocionalmente | Definición | ◆◆ |
| F040 | Eufemismo encubridor | Definición | ◆◆ |
| F041 | Ad hominem abusivo | Relevancia | ◆◆◆ |
| F042 | Ad hominem circunstancial | Relevancia | ◆◆ |
| F043 | Tu quoque | Relevancia | ◆◆◆ |
| F044 | Bulverismo | Relevancia | ◆◆ |
| F045 | Envenenamiento del pozo | Relevancia | ◆◆◆ |
| F046 | Falacia genética | Relevancia | ◆◆◆ |
| F047 | Ad baculum | Relevancia | ◆◆◆ |
| F048 | Ad misericordiam | Relevancia | ◆◆ |
| F049 | Ad populum | Relevancia | ◆◆◆ |
| F050 | Efecto de arrastre | Relevancia | ◆◆ |
| F051 | Ad verecundiam fuera de dominio | Relevancia | ◆◆◆ |
| F052 | Apelación a autoridad anónima | Relevancia | ◆◆◆ |
| F053 | Consenso fabricado | Relevancia | ◆◆◆ |
| F054 | Ad ignorantiam | Relevancia | ◆◆◆ |
| F055 | Ad antiquitatem | Relevancia | ◆◆ |
| F056 | Ad novitatem | Relevancia | ◆◆ |
| F057 | Ad naturam | Relevancia | ◆◆ |
| F058 | Ad metum | Relevancia | ◆◆◆ |
| F059 | Apelación al asco | Relevancia | ◆◆ |
| F060 | Ad crumenam | Relevancia | ◆◆ |
| F061 | Ad lazarum | Relevancia | ◆◆ |
| F062 | Ad captandum vulgus | Relevancia | ◆◆ |
| F063 | Ad odium | Relevancia | ◆◆◆ |
| F064 | Apelación al ridículo | Relevancia | ◆◆◆ |
| F065 | Apelación al misterio | Relevancia | ◆◆ |
| F066 | Argumentum ex silentio | Relevancia | ◆◆ |
| F067 | Hombre de paja | Distorsión | ◆◆◆ |
| F068 | Motte and bailey | Distorsión | ◆◆◆ |
| F069 | Red herring | Distorsión | ◆◆◆ |
| F070 | Ignoratio elenchi | Distorsión | ◆◆◆ |
| F071 | Decontextualización | Distorsión | ◆◆◆ |
| F072 | Lógica de la olla | Distorsión | ◆◆ |
| F073 | Kafkatrapping | Distorsión | ◆◆◆ |
| F074 | Petición de principio | Presunción | ◆◆◆ |
| F075 | Pregunta compleja | Presunción | ◆◆◆ |
| F076 | Falsa dicotomía | Presunción | ◆◆◆ |
| F077 | Punto medio falso | Presunción | ◆◆◆ |
| F078 | Carga de prueba desplazada | Presunción | ◆◆◆ |
| F079 | Pendiente resbaladiza conceptual | Presunción | ◆◆ |
| F080 | Pendiente resbaladiza causal | Presunción | ◆◆ |
| F081 | Post hoc ergo propter hoc | Causalidad | ◆◆◆ |
| F082 | Cum hoc ergo propter hoc | Causalidad | ◆◆◆ |
| F083 | Inversión causa/efecto | Causalidad | ◆◆◆ |
| F084 | Causa común ignorada | Causalidad | ◆◆◆ |
| F085 | Sobredeterminación causal ignorada | Causalidad | ◆◆ |
| F086 | Agente único | Causalidad | ◆◆ |
| F087 | Non causa pro causa | Causalidad | ◆◆◆ |
| F088 | Generalización apresurada | Inducción | ◆◆◆ |
| F089 | Accidente | Inducción | ◆◆◆ |
| F090 | Accidente inverso | Inducción | ◆◆◆ |
| F091 | Sesgo de selección | Inducción | ◆◆◆ |
| F092 | Generalización desde casos típicos | Inducción | ◆◆ |
| F093 | Argumento desde incredulidad personal | Inducción | ◆◆◆ |
| F094 | Argumento desde la complejidad | Inducción | ◆◆ |
| F095 | No verdadero escocés | Definición | ◆◆◆ |
| F096 | Falacia del umbral / sorites | Definición | ◆◆ |
| F097 | Error de categoría | Epistémica | ◆◆◆ |
| F098 | Falacia de la esencia | Definición | ◆◆ |
| F099 | Falsa analogía | Analogía | ◆◆◆ |
| F100 | Analogía por semejanza superficial | Analogía | ◆◆◆ |
| F101 | Analogía extendida indebidamente | Analogía | ◆◆ |
| F102 | Metáfora cosificada | Analogía | ◆◆ |
| F103 | Analogía legal defectuosa | Analogía | ◆◆◆ |
| F104 | Analogía moral defectuosa | Analogía | ◆◆◆ |
| F105 | Analogía por resultado | Analogía | ◆◆ |
| F106 | Única explicación | Abducción | ◆◆◆ |
| F107 | Navaja de Ockham mal aplicada | Abducción | ◆◆ |
| F108 | Rescate ad hoc | Abducción | ◆◆◆ |
| F109 | Confirmación circular abductiva | Abducción | ◆◆◆ |
| F110 | Pensamiento mágico | Abducción | ◆◆◆ |
| F111 | Irreproducibilidad como confirmación | Abducción | ◆◆◆ |
| F112 | Abducción desde la ignorancia | Abducción | ◆◆◆ |
| F113 | Francotirador de Texas | Estadística | ◆◆◆ |
| F114 | Cherry-picking | Estadística | ◆◆◆ |
| F115 | Sesgo de confirmación argumentativo | Estadística | ◆◆◆ |
| F116 | Superviviente | Estadística | ◆◆◆ |
| F117 | Anecdotario | Estadística | ◆◆◆ |
| F118 | Ecológica | Estadística | ◆◆◆ |
| F119 | Falacia del jugador | Probabilidad | ◆◆◆ |
| F120 | Mano caliente | Probabilidad | ◆◆ |
| F121 | Ignorancia de la tasa base | Probabilidad | ◆◆◆ |
| F122 | Falacia del fiscal | Probabilidad | ◆◆◆ |
| F123 | Falacia del defensor | Probabilidad | ◆◆◆ |
| F124 | Falacia de la conjunción | Probabilidad | ◆◆◆ |
| F125 | Falacia de la disyunción | Probabilidad | ◆◆ |
| F126 | Paradoja de Simpson | Probabilidad | ◆◆◆ |
| F127 | Falacia de la precisión | Probabilidad | ◆◆ |
| F128 | Regresión a la media ignorada | Probabilidad | ◆◆◆ |
| F129 | Falacia lúdica | Probabilidad | ◆◆◆ |
| F130 | Falacia narrativa | Probabilidad | ◆◆ |
| F131 | Porcentaje sin base | Estadística | ◆◆◆ |
| F132 | Falacia del 95% | Estadística | ◆◆◆ |
| F133 | HARKing | Estadística | ◆◆◆ |
| F134 | Enumeración perfecta falsa | Inducción | ◆◆◆ |
| F135 | Inducción por confirmación exclusiva | Inducción | ◆◆◆ |
| F136 | Regla sin excepción | Inducción | ◆◆ |
| F137 | Proyección mental | Epistémica | ◆◆◆ |
| F138 | Ilusión de comprensión | Epistémica | ◆◆ |
| F139 | Vocabulario técnico como evidencia | Epistémica | ◆◆◆ |
| F140 | Descripción como conocimiento directo | Epistémica | ◆◆ |
| F141 | Razonamiento motivado | Epistémica | ◆◆◆ |
| F142 | Doble rasero epistémico | Epistémica | ◆◆◆ |
| F143 | Falso balance | Epistémica | ◆◆◆ |
| F144 | Deepity | Epistémica | ◆◆ |
| F145 | Profundidad ilusoria de la paradoja | Epistémica | ◆ |
| F146 | Falacia naturalista | Moral | ◆◆◆ |
| F147 | Falacia moralista | Moral | ◆◆◆ |
| F148 | Consenso moral | Moral | ◆◆ |
| F149 | Relativismo moral empírico | Moral | ◆◆◆ |
| F150 | Omission bias | Moral | ◆◆ |
| F151 | Doble efecto mal aplicado | Moral | ◆◆ |
| F152 | Persona completa | Moral | ◆◆ |
| F153 | Legalidad moral | Moral | ◆◆◆ |
| F154 | Universalización kantiana incorrecta | Moral | ◆◆ |
| F155 | Hecho bruto como justificación | Moral | ◆◆◆ |
| F156 | Carga de prueba invertida | Dialéctica | ◆◆◆ |
| F157 | Ad consequentiam | Dialéctica | ◆◆◆ |
| F158 | Falacia pragmática | Dialéctica | ◆◆ |
| F159 | Mover los postes | Dialéctica | ◆◆◆ |
| F160 | Aplazamiento infinito | Dialéctica | ◆◆ |
| F161 | Silencio como consentimiento | Dialéctica | ◆◆ |
| F162 | Distracción dialéctica | Dialéctica | ◆◆◆ |
| F163 | Galileo gambit | Dialéctica | ◆◆◆ |
| F164 | Pregunta retórica como argumento | Dialéctica | ◆◆ |
| F165 | Compromiso explotado | Dialéctica | ◆◆ |
| F166 | Cambio de contexto de diálogo | Dialéctica | ◆◆ |
| F167 | Posición de autoridad en diálogo | Dialéctica | ◆◆◆ |
| F168 | Dilución del argumento | Dialéctica | ◆◆ |
| F169 | Definición por ejemplo | Definición | ◆ |
| F170 | Definición negativa exclusiva | Definición | ◆ |
| F171 | Cambio de significado | Ambigüedad | ◆◆◆ |
| F172 | Argumento de la etiqueta | Definición | ◆◆ |
| F173 | Excepción confirmatoria | Científica | ◆◆◆ |
| F174 | Cherry-picking de estudios | Científica | ◆◆◆ |
| F175 | Confusión modelo/realidad | Científica | ◆◆◆ |
| F176 | Extrapolación más allá del dominio | Científica | ◆◆◆ |
| F177 | Laboratorio a mundo real | Científica | ◆◆ |
| F178 | Contrarianism fallacy | Científica | ◆◆ |
| F179 | Composición económica | Económica | ◆◆◆ |
| F180 | Crecimiento infinito | Económica | ◆◆ |
| F181 | Consecuencias económicas como verdad | Económica | ◆◆ |
| F182 | Costo hundido | Económica | ◆◆◆ |
| F183 | Costo de oportunidad ignorado | Económica | ◆◆ |
| F184 | Suma cero | Económica | ◆◆◆ |
| F185 | Ad hominem de fuente legal | Jurídica | ◆◆ |
| F186 | Pendiente jurídica | Jurídica | ◆◆ |
| F187 | Precedente irrelevante | Jurídica | ◆◆◆ |
| F188 | Norma como descripción | Jurídica | ◆◆◆ |
| F189 | Mandato popular | Jurídica | ◆◆ |
| F190 | Estado de naturaleza | Jurídica | ◆◆ |
| F191 | Anclaje argumentativo | Cognitiva | ◆◆ |
| F192 | Efecto de encuadre | Cognitiva | ◆◆◆ |
| F193 | Efecto halo | Cognitiva | ◆◆ |
| F194 | Sesgo de status quo | Cognitiva | ◆◆ |
| F195 | Punto de referencia | Cognitiva | ◆◆ |
| F196 | Screenshot como prueba | Digital | ◆◆◆ |
| F197 | Trending como verdad | Digital | ◆◆◆ |
| F198 | Cámara de eco | Digital | ◆◆◆ |
| F199 | Cherry-picking algorítmico | Digital | ◆◆◆ |
| F200 | Like como endorsement | Digital | ◆◆ |
| F201 | Verificado como veraz | Digital | ◆◆◆ |
| F202 | Palimpsesto digital | Digital | ◆◆ |
| F203 | Deepfake como argumento | Digital | ◆◆◆ |
| F204 | Autoridad de la plataforma | Digital | ◆◆ |
| F205 | Falacia del detector de falacias | Meta | ◆◆◆ |
| F206 | Etiqueta sin demostración | Meta | ◆◆◆ |
| F207 | Falacia de la falacia de la falacia | Meta | ◆◆ |
| F208 | Formalismo estricto | Meta | ◆◆ |
| F209 | Solución perfecta | Meta | ◆◆◆ |
| F210 | (ver F163 — Galileo gambit) | Meta | — |
| F211 | Dilución argumental | Meta | ◆◆ |
| F212 | Escala manipulada en gráfica | Visual | ◆◆◆ |
| F213 | Correlación visual espuria | Visual | ◆◆◆ |
| F214 | Imagen sesgada | Visual | ◆◆◆ |
| F215 | Porcentaje visual | Visual | ◆◆ |
| F216 | Apelación visual a la autoridad | Visual | ◆◆ |
| F217–F222 | Variantes de apelación a la autoridad | Autoridad | ◆◆ – ◆◆◆ |
| F223 | Conspiranoia abductiva | Abducción | ◆◆◆ |
| F224 | Patrón en el ruido | Abducción | ◆◆◆ |
| F225 | Anecdótica como abducción | Abducción | ◆◆◆ |
| F226 | Congeries | Retórica | ◆◆ |
| F227 | Exordio tendencioso | Retórica | ◆ |
| F228 | Ethos fallacy | Retórica | ◆◆ |
| F229 | Quaestio | Retórica | ◆◆◆ |

---

## APÉNDICE II — CRITERIOS PARA UN DETECTOR NO LÉXICO

Para construir un sistema de detección que razone sobre estructura y función argumentativa, no sobre palabras clave:

**Dimensión 1 — Análisis estructural inferencial**
¿La conclusión se sigue de las premisas por alguna regla de inferencia válida? Mapear la forma lógica subyacente.

**Dimensión 2 — Relevancia de las premisas**
¿Son las premisas semántica y pragmáticamente pertinentes para la conclusión? Evaluar la distancia topológica entre premisa y conclusión.

**Dimensión 3 — Suficiencia evidencial**
¿Son las premisas suficientes para sostener la conclusión o hay saltos injustificados? ¿Qué premisas implícitas serían necesarias para hacer el argumento válido?

**Dimensión 4 — Carga de la prueba**
¿Quién afirma qué y ha ofrecido evidencia proporcionada a la fuerza de su afirmación? ¿Se ha invertido ilegítimamente la carga?

**Dimensión 5 — Análisis del contexto dialógico**
¿Qué tipo de intercambio es? (debate, deliberación, negociación, investigación) ¿Se respetan sus normas pragmáticas propias?

**Dimensión 6 — Carga semántica y definiciones**
¿Hay términos ambiguos, emotivos, equivocados o redefinidos que distorsionen el argumento? ¿Los términos clave mantienen el mismo significado a lo largo del argumento?

**Dimensión 7 — Presupuestos implícitos**
¿Qué se da por sentado sin demostrar? Extracción de las premisas implícitas necesarias para validar cada paso inferencial.

**Dimensión 8 — Análisis causal**
¿Se infiere causalidad de correlación temporal, simultánea o espacial? ¿Se han considerado causas comunes? ¿Se ha invertido la dirección causal?

**Dimensión 9 — Representatividad de la evidencia**
¿Es la evidencia sistemática, anecdótica o sesgada? ¿Qué fracción del espacio evidencial ha sido considerada? ¿Hay sesgo de superviviente o de publicación?

**Dimensión 10 — Coherencia diacrónica**
¿El interlocutor mantiene posiciones consistentes o cambia los criterios según conveniencia (mover los postes, motte and bailey)?

**Dimensión 11 — Análisis modal**
¿Se confunde lo posible con lo probable, o lo probable con lo necesario? ¿Se infieren necesidades de regularidades contingentes?

**Dimensión 12 — Análisis cuantitativo de la afirmación**
¿Las afirmaciones cuantitativas están respaldadas por estadísticas válidas? ¿Se usan correctamente las tasas base, los intervalos de confianza y el tamaño del efecto?

---

*Total de falacias codificadas: 229 entradas con código único.*
*Tradición filosófica cubierta: Aristóteles (Refutaciones Sofísticas) · John Locke · David Hume · G.E. Moore · Bertrand Russell · Gilbert Ryle · Charles Hamblin (Fallacies, 1970) · Douglas Walton · John Woods · Frans van Eemeren & Rob Grootendorst (Pragma-dialéctica) · Ralph Johnson & J. Anthony Blair · Daniel Kahneman & Amos Tversky · Nassim Nicholas Taleb · Daniel Dennett · Jonathan Haidt.*
