"""Authorization system.

The authorization system is as simple as possible. It is specified as part of
a YAML model and based around the email address retrieved as part of the
OAuth authentication process. Email addresses are then assigned to groups,
and each dataset specifies the groups which shall have read access to it.

Additionally, two pseudo-group exists: "Anybody" for all site visitors, and
"Users" for those who have signed into the site.
"""
from memorious.model.datasets import Dataset


class Group(object):
    """A group of users with a set of members."""

    def __init__(self, name, members):
        self.name = name
        self.members = [self.normalise_user(u) for u in members]

    def normalise_user(self, user):
        if user is None:
            return
        return user.lower().strip()

    def is_member(self, user):
        return self.normalise_user(user) in self.members


class AnybodyGroup(Group):
    """Anyone."""

    NAME = 'anybody'

    def __init__(self):
        super(AnybodyGroup, self).__init__(self.NAME, [])

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

        for group in model.groups + [UsersGroup(), AnybodyGroup()]:
            if group.is_member(user):
                self.groups.append(group.name)

    def has_access(self, dataset):
        if not isinstance(dataset, Dataset):
            dataset = self.model.get_dataset(dataset)
        for group in dataset.groups:
            if group in self.groups:
                return True
        return False

    def __repr__(self):
        return '<Auth(%r, %r)>' % (self.user, self.groups)
