

class Group(object):
    """A group of users with a set of members."""

    def __init__(self, name, members):
        self.name = name
        self.members = [self.normalise_user(u) for u in members]

    def normalise_user(self, user):
        return user.lower().strip()

    def is_member(self, user):
        return self.normalise_user(user) in self.members


class VisitorsGroup(Group):
    """Anyone."""

    NAME = 'visitors'

    def __init__(self):
        super(VisitorsGroup, self).__init__(self.NAME, [])

    def is_member(self, user):
        return True


class UsersGroup(Group):
    """All logged-in users."""

    NAME = 'users'

    def __init__(self):
        super(UsersGroup, self).__init__(self.NAME, [])

    def is_member(self, user):
        return user is not None


class Auth(object):
    """Handle the authorization state of a given request.

    This object is used to compute and pass around the group permissions
    that a given user has within the context of a specific request or
    processing context. It is used to filter queries.
    """

    def __init__(self, user, model):
        self.user = user
        self.model = model
        self.groups = []

        for group in model.groups + [UsersGroup(), VisitorsGroup()]:
            if group.is_member(user):
                self.groups.append(group.name)

    def __repr__(self):
        return '<Auth(%r, %r)>' % (self.user, self.groups)
