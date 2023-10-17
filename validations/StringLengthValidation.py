
class StringLengthValidation:

    @classmethod
    def validate(cls, data: dict, rules: dict):
        """
        Function validate
        param dict data
        param dict rules
        returns dict
        """
        valid = True
        if valid:
            return {}
        return {"valid": False, "msg": rules['msg']}
