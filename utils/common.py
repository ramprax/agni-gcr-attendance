def sanitizeEmail(email):
    email = email.strip().lower()
    emailParts = email.split('@')

    if len(emailParts) != 2:
        raise Exception('Invalid email id: %s', email)

    for ep in emailParts:
        if not ep:
            raise Exception('Invalid email id: %s', email)

    emailDomainParts = emailParts[1].split('.')

    if len(emailDomainParts) <= 1:
        raise Exception('Invalid email id: %s', email)

    for ed in emailDomainParts:
        if not ed:
            raise Exception('Invalid email id: %s', email)

    return email