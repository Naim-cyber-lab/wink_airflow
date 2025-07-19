-- Supprimer les votes liés aux options de sondage des messages des conversations expirées
DELETE FROM profil_voteconversation
WHERE poll_option_id IN (
    SELECT poc.id
    FROM profil_polloptionconversation poc
    JOIN profil_pollconversation pc ON poc.poll_id = pc.id
    JOIN profil_conversationactivitymessages cam ON pc.conversation_activity_message_id = cam.id
    JOIN profil_conversationactivity ca ON cam.conversationActivity_id = ca.id
    WHERE ca.date < NOW()
);

-- Supprimer les options de sondage
DELETE FROM profil_polloptionconversation
WHERE poll_id IN (
    SELECT pc.id
    FROM profil_pollconversation pc
    JOIN profil_conversationactivitymessages cam ON pc.conversation_activity_message_id = cam.id
    JOIN profil_conversationactivity ca ON cam.conversationActivity_id = ca.id
    WHERE ca.date < NOW()
);

-- Supprimer les sondages
DELETE FROM profil_pollconversation
WHERE conversation_activity_message_id IN (
    SELECT cam.id
    FROM profil_conversationactivitymessages cam
    JOIN profil_conversationactivity ca ON cam.conversationActivity_id = ca.id
    WHERE ca.date < NOW()
);

-- Supprimer les messages
DELETE FROM profil_conversationactivitymessages
WHERE conversationActivity_id IN (
    SELECT id FROM profil_conversationactivity WHERE date < NOW()
);

-- Supprimer les participants
DELETE FROM profil_participantconversationactivity
WHERE conversationActivity_id IN (
    SELECT id FROM profil_conversationactivity WHERE date < NOW()
);

-- Supprimer les conversations expirées
DELETE FROM profil_conversationactivity
WHERE date < NOW();
