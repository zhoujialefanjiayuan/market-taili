from enum import Enum, unique


class BaseEnum(Enum):

    @classmethod
    def choices(cls):
        return [e.value for e in cls]


@unique
class CustomerServiceApplicationStatus(BaseEnum):
    EXIST = 0  # 已存在,未提交电核
    UNCLAIMED = 1  # 已提交,待领取
    VERIFYING = 2  # 电核中
    VERIFY_DONE = 3  # 电核完成
    DONE = 4  # 处理完成

    @classmethod
    def visible(cls):
        return [cls.UNCLAIMED.value, cls.VERIFY_DONE.value,
                cls.VERIFYING.value, cls.DONE.value]


@unique
class ApplicationStatus(BaseEnum):
    test = (1,"test")
    waitcheck = (0,'待审核')
    checking = (10,'审核中')
    abandon = (20,'用户放弃')
    refuse = (30,'审核拒绝')
    failremittance = (60, '放款失败')

    waitremittance = (40,'待放款')
    remitting = (50,'放款中')
    repaying = (70,'还款中')
    successrepay = (80,'还款成功')
    repayedpart = (130, '部分还，待确认')
    repayedall = (120, '还清，待确认')

    overdue = (90,'逾期')
    overduerepayed = (100,'逾期已还款')
    overduebad = (110,'坏账')



    @classmethod
    def visible_dict(cls):
        return  {k:v for k,v in [cls.test.value,cls.waitcheck.value,cls.checking.value,cls.abandon.value,cls.refuse.value,
                  cls.waitremittance.value,cls.remitting.value,cls.failremittance.value,cls.repaying.value,
                  cls.successrepay.value,cls.overdue.value,cls.overduerepayed.value,cls.overduebad.value,
                  cls.repayedall.value,cls.repayedpart.value,]}



