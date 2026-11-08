package demo.saga.common;

/**
 * Erreur NON transitoire : réessayer ne servira à rien (JSON invalide, donnée métier incohérente...).
 * Un handler peut la lancer (ou l'émettre dans son Mono) pour envoyer immédiatement
 * le message en Dead Letter Topic, sans passer par les réessais.
 */
public class NonRetryableException extends RuntimeException {

    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
